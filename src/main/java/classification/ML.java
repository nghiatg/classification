package classification;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

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
		
	// this tfidf use countvectorizer not hashingtf
	public static Dataset<Row> cv(Dataset<Row> dataset, String vocabPath) throws Exception { 
		CountVectorizerModel cvModel = new CountVectorizer().setInputCol("raw").setOutputCol("tf").setMinDF(1).fit(dataset);
		Dataset<Row> afterCV = cvModel.transform(dataset);
		Utils.writeVocab(cvModel.vocabulary(), vocabPath);
		return afterCV;
	}
	
	public static Dataset<Row> tfidf(Dataset<Row> afterCV) throws Exception { 
		IDFModel idfModel = new IDF().setInputCol("tf").setOutputCol("tfidf").fit(afterCV);
		return idfModel.transform(afterCV);
	}
	
	public static Dataset<Row> createDataSet() throws Exception {
		ArrayList<Row> rows = new ArrayList<Row>();
		BufferedReader br = new BufferedReader(new FileReader("data_nosw.txt"));
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
	
	

}
