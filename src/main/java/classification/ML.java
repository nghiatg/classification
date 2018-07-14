package classification;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
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
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Int;
import scala.Tuple2;

public class ML {
	static String cvModelPath = "model/cvmodel";
	static String idfModelPath = "model/idfmodel";
	static String nbModelPath = "model/nbmodel";
	static SparkSession spark = SparkSession.builder().appName("JavaPipelineExample").master("local[*]").getOrCreate();
	static int vocabLength;
	static {
		try {
			vocabLength = Utils.getVocabLength();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// this tfidf use countvectorizer not hashingtf
	public static Dataset<Row> cv(Dataset<Row> dataset, String vocabPath) throws Exception {
		CountVectorizerModel cvModel = new CountVectorizer().setInputCol("raw").setOutputCol("tf").setMinDF(1)
				.fit(dataset);
		cvModel.save(cvModelPath);
		Dataset<Row> afterCV = cvModel.transform(dataset);
		Utils.writeVocab(cvModel.vocabulary(), vocabPath);
		vocabLength = Utils.getVocabLength();
		return afterCV;
	}

	public static Dataset<Row> tfidf(Dataset<Row> afterCV) throws Exception {
		IDFModel idfModel = new IDF().setInputCol("tf").setOutputCol("features").fit(afterCV);
		idfModel.save(idfModelPath);
		return idfModel.transform(afterCV);
	}

	public static Dataset<Row> createDataSet(String inputFile) throws Exception {
		ArrayList<Row> rows = new ArrayList<Row>();
		BufferedReader br = new BufferedReader(new FileReader(inputFile));
		String line = br.readLine();
		int id = 0;
		while (line != null) {
			rows.add(Utils.changeStringToRowWithLabel(id, Integer.parseInt(line.split("\t")[0]),line.split("\t")[1]));
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

	public static void oneVsRest(String input) throws Exception {
		// load data file.
		Dataset<Row> inputData = spark.read().format("libsvm").load(input);

		// generate the train/test split.
		Dataset<Row>[] tmp = inputData.randomSplit(new double[] { 0.6, 0.4 });
		Dataset<Row> train = tmp[0];
		Dataset<Row> test = tmp[1];

		// configure the base classifier.
		LogisticRegression classifier = new LogisticRegression().setMaxIter(100).setTol(1E-6).setFitIntercept(true);

		// instantiate the One Vs Rest Classifier.
		OneVsRest ovr = new OneVsRest().setClassifier(classifier);

		// train the multiclass model.
		OneVsRestModel ovrModel = ovr.fit(train);

		// score the model on test data.
		Dataset<Row> predictions = ovrModel.transform(test).select("label", "prediction");
		predictions.sort("label").show(10000);

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
		Dataset<Row>[] splits = dataFrame.randomSplit(new double[] { 0.6, 0.4 }, 1234L);
		Dataset<Row> train = splits[0];
		Dataset<Row> test = splits[1];

		// create the trainer and set its parameters
		NaiveBayes nb = new NaiveBayes();

		// train the model
		NaiveBayesModel model = nb.fit(train);
		model.save(nbModelPath);

		// Select example rows to display.
		Dataset<Row> predictions = model.transform(test);
		predictions.sort("label").select("label", "prediction").show(10000, false);

		// compute accuracy on the test set
		MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator().setLabelCol("label")
				.setPredictionCol("prediction").setMetricName("accuracy");
		double accuracy = evaluator.evaluate(predictions);
		System.out.println("Test set accuracy = " + accuracy);

		// for evaluation f1
		JavaPairRDD<Object, Object> predictionRDD = predictions.select("label", "prediction").toJavaRDD()
				.mapToPair(new PairFunction<Row, Object, Object>() {
					public Tuple2<Object, Object> call(Row row) throws Exception {
						return new Tuple2<Object, Object>((Object) row.get(0), (Object) row.get(1));
					}
				});
		// Get evaluation metrics.
		MulticlassMetrics metrics = new MulticlassMetrics(predictionRDD.rdd());

		// Confusion matrix
		Matrix confusion = metrics.confusionMatrix();
		System.out.println("Confusion matrix: \n" + confusion.toString(Int.MaxValue(),Int.MaxValue()));
		
		// Overall statistics
		System.out.println("Accuracy = " + metrics.accuracy());

		// Stats by labels
		System.out.println("Class\t\t\tPrecision\tRecall\t\tF1");
		for (int i = 0; i < metrics.labels().length; i++) {
			System.out.println("Class  " + i + "\t\t" + Utils.formatDouble(metrics.precision(metrics.labels()[i]),3) + " \t\t"
					+ Utils.formatDouble(metrics.recall(metrics.labels()[i]),3) + " \t\t" + Utils.formatDouble(metrics.fMeasure(metrics.labels()[i]),3));
		}
	}

	public static void mlp() throws Exception {
		Dataset<Row> dataFrame = spark.read().format("libsvm").load("data//data_fc_shuffle");

		// Split the data into train and test
		Dataset<Row>[] splits = dataFrame.randomSplit(new double[] { 0.7, 0.3 }, 1234L);
		Dataset<Row> train = splits[0];
		Dataset<Row> test = splits[1];

		// specify layers for the neural network:
		// input layer of size 4 (features), two intermediate of size 5 and 4
		// and output of size 22 (classes)
		int[] layers = new int[] { 4, 5, 4, 22 };

		// create the trainer and set its parameters
		MultilayerPerceptronClassifier trainer = new MultilayerPerceptronClassifier().setLayers(layers)
				.setBlockSize(128).setSeed(1234L).setMaxIter(100);

		// train the model
		MultilayerPerceptronClassificationModel model = trainer.fit(train);

		// compute accuracy on the test set
		Dataset<Row> result = model.transform(test);
		Dataset<Row> predictionAndLabels = result.select("prediction", "label");
		predictionAndLabels.show();
		MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy");

		System.out.println("Test set accuracy = " + evaluator.evaluate(predictionAndLabels));
	}
	
	public static CountVectorizerModel getCvModel() throws Exception { 
		return CountVectorizerModel.load(cvModelPath);
	}
	public static IDFModel getIdfModel() throws Exception { 
		return IDFModel.load(idfModelPath);
	}

	public static NaiveBayesModel getNbModel() throws Exception { 
		return NaiveBayesModel.load(nbModelPath);
	}
	
	public static void predictDocs(ArrayList<String> rawDocs) throws Exception { 
		// preprocess docs
		PreProcessing pp = new PreProcessing();
		ArrayList<String> ppdocs = new ArrayList<String>();
		for(String raw : rawDocs) {
			ppdocs.add(pp.process(raw));
		}
		Dataset<Row> firstDataset = createDataset(ppdocs);
		CountVectorizerModel cvModel = getCvModel();
		IDFModel idfModel = getIdfModel();
		NaiveBayesModel nbModel = getNbModel();
		Dataset<Row> afterTfidf = idfModel.transform(cvModel.transform(firstDataset));
		
		// TODO 
		Dataset<Row> predictions = nbModel.transform(afterTfidf);
		predictions.show(false);
	}
	
	public static Dataset<Row> createDataset(ArrayList<String> docs) throws Exception { 
		ArrayList<Row> rows = new ArrayList<Row>();
		int id = 0;
		for(String doc : docs) {
			rows.add(Utils.changeStringToRowWithNoLabel(id,doc));
			id++;
		}
		StructType schema = new StructType(new StructField[] {
				new StructField("id", DataTypes.LongType, false, Metadata.empty()),
				new StructField("raw", DataTypes.createArrayType(DataTypes.StringType), false, Metadata.empty()) });
		Dataset<Row> dataset = spark.createDataFrame(rows, schema);
		return dataset;
	}
	
	

}
