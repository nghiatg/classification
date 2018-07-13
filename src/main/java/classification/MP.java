package classification;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class MP {

	public static void main(String[] args) throws Exception {
		ML.spark.sparkContext().setLogLevel("ERROR");
		realMain();
//		testMain();
	}
	
	public static void realMain() throws Exception { 
//		Data.removeSw();
//		Dataset<Row> rawDataIntoDataset = ML.createDataSet();
//		Dataset<Row> afterCV = ML.cv(rawDataIntoDataset,"data//vocabulary");
//		Dataset<Row> tfidf = ML.tfidf(afterCV);
//		tfidf.cache();
//		tfidf.printSchema();
//		Data.writeDataForClassification(tfidf.select("label","tfidf"), "data//data_fc");
//		Utils.shuffle("data//data_fc", "data//data_fc_shuffle");
		ML.NB("data//data_fc_shuffle");
//		ML.oneVsRest();
//		ML.mlp();
//		Utils.changeLabel();
//		Utils.getLackData();
//		Data.getOtherSourceData();
	}
	public static void testMain() throws Exception { 
		Utils.testDataSample();
	}
	

}
