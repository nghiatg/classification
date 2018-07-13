package classification;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel;
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.classification.OneVsRest;
import org.apache.spark.ml.classification.OneVsRestModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ML {
	static SparkSession spark = SparkSession.builder().appName("JavaPipelineExample").master("local[*]").getOrCreate();
	static int vocabLength;
	static {
		try {
			vocabLength = Utils.getVocabLength();
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
		
	// this tfidf use countvectorizer not hashingtf
	public static Dataset<Row> cv(Dataset<Row> dataset, String vocabPath) throws Exception { 
		CountVectorizerModel cvModel = new CountVectorizer().setInputCol("raw").setOutputCol("tf").setMinDF(1).fit(dataset);
		Dataset<Row> afterCV = cvModel.transform(dataset);
		Utils.writeVocab(cvModel.vocabulary(), vocabPath);
		vocabLength = Utils.getVocabLength();
		return afterCV;
	}
	
	public static Dataset<Row> tfidf(Dataset<Row> afterCV) throws Exception { 
		IDFModel idfModel = new IDF().setInputCol("tf").setOutputCol("tfidf").fit(afterCV);
		return idfModel.transform(afterCV);
	}
	
	public static Dataset<Row> createDataSet() throws Exception {
		ArrayList<Row> rows = new ArrayList<Row>();
		BufferedReader br = new BufferedReader(new FileReader("data//data_nosw.txt"));
		String line = br.readLine();
		int id = 0;
		while(line != null) {
			rows.add(Utils.changeStringToRow(line.split("\t")[1], id, Integer.parseInt(line.split("\t")[0])));
			id++;
			line = br.readLine();
		}
		br.close();
		StructType schema = new StructType(new StructField[] {
				new StructField("id", DataTypes.LongType, false, Metadata.empty()),
				new StructField("label", DataTypes.IntegerType, false, Metadata.empty()),
				new StructField("raw", DataTypes.createArrayType(DataTypes.StringType), false, Metadata.empty()) });
		Dataset<Row> dataset = spark.createDataFrame(rows, schema);
		return dataset;

	}
	
	public static void oneVsRest() throws Exception { 
		// load data file.
		Dataset<Row> inputData = spark.read().format("libsvm").load("data//data_fc_shuffle");

		// generate the train/test split.
		Dataset<Row>[] tmp = inputData.randomSplit(new double[]{0.6, 0.4});
		Dataset<Row> train = tmp[0];
		Dataset<Row> test = tmp[1];

		// configure the base classifier.
		LogisticRegression classifier = new LogisticRegression()
		  .setMaxIter(100)
		  .setTol(1E-6)
		  .setFitIntercept(true);

		// instantiate the One Vs Rest Classifier.
		OneVsRest ovr = new OneVsRest().setClassifier(classifier);

		// train the multiclass model.
		OneVsRestModel ovrModel = ovr.fit(train);

		// score the model on test data.
		Dataset<Row> predictions = ovrModel.transform(test).select("prediction", "label");
		predictions.show(10000);

		// obtain evaluator.
		MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy");

		// compute the classification error on test data.
		double accuracy = evaluator.evaluate(predictions);
		System.out.println("Test Accuracy = " + accuracy);
	}
	
	public static void NB(String inputFile) throws Exception { 
		// Load training data
		Dataset<Row> dataFrame = spark.read().format("libsvm").load(inputFile);
		// Split the data into train and test
		Dataset<Row>[] splits = dataFrame.randomSplit(new double[]{0.6, 0.4}, 1234L);
		Dataset<Row> train = splits[0];
		Dataset<Row> test = splits[1];

		// create the trainer and set its parameters
		NaiveBayes nb = new NaiveBayes();

		// train the model
		NaiveBayesModel model = nb.fit(train);

		// Select example rows to display.
		Dataset<Row> predictions = model.transform(test);
		predictions.select("label","prediction").show(10000,false);

		// compute accuracy on the test set
		MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
		  .setLabelCol("label")
		  .setPredictionCol("prediction")
		  .setMetricName("accuracy");
		double accuracy = evaluator.evaluate(predictions);
		System.out.println("Test set accuracy = " + accuracy);
	}
	
	public static void mlp() throws Exception { 
		Dataset<Row> dataFrame = spark.read().format("libsvm").load("data//data_fc_shuffle");

		// Split the data into train and test
		Dataset<Row>[] splits = dataFrame.randomSplit(new double[]{0.7, 0.3}, 1234L);
		Dataset<Row> train = splits[0];
		Dataset<Row> test = splits[1];

		// specify layers for the neural network:
		// input layer of size 4 (features), two intermediate of size 5 and 4
		// and output of size 22 (classes)
		int[] layers = new int[] {4, 5, 4, 22};

		// create the trainer and set its parameters
		MultilayerPerceptronClassifier trainer = new MultilayerPerceptronClassifier()
		  .setLayers(layers)
		  .setBlockSize(128)
		  .setSeed(1234L)
		  .setMaxIter(100);

		// train the model
		MultilayerPerceptronClassificationModel model = trainer.fit(train);

		// compute accuracy on the test set
		Dataset<Row> result = model.transform(test);
		Dataset<Row> predictionAndLabels = result.select("prediction", "label");
		predictionAndLabels.show();
		MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
		  .setMetricName("accuracy");

		System.out.println("Test set accuracy = " + evaluator.evaluate(predictionAndLabels));
	}
	

}
