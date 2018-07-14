package classification;

import java.io.File;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class MP {

	public static void main(String[] args) throws Exception {
		ML.spark.sparkContext().setLogLevel("ERROR");
		realMain();
//		testMain();
	}
	
	public static void realMain() throws Exception { 
		Utils.deleteDir(new File(ML.cvModelPath.replace("/", "\\")));
		Utils.deleteDir(new File(ML.idfModelPath.replace("/", "\\")));
		Utils.deleteDir(new File(ML.nbModelPath.replace("/", "\\")));
		Data.removeSw("data//datatrain_more_pped_changeLabel_limit.txt","data//data_nosw.txt");
		Dataset<Row> rawDataIntoDataset = ML.createDataSet("data//data_nosw.txt");
		Dataset<Row> afterCV = ML.cv(rawDataIntoDataset,"data//vocabulary");
		Dataset<Row> tfidf = ML.tfidf(afterCV);
		tfidf.cache();
		tfidf.printSchema();
		Data.writeDataForClassification(tfidf.select("label","tfidf"), "data//data_fc");
//		Utils.shuffle("data//data_fc", "data//data_fc_shuffle");
		ML.NB("data//data_fc");
//		ML.oneVsRest("data//data_fc_shuffle");
//		ML.mlp();
//		Utils.changeLabel();
//		Utils.getLackData();
//		Data.getOtherSourceData();
//		Data.limitData("data//datatrain_more_pped_changeLabel.txt", "data//datatrain_more_pped_changeLabel__limit.txt", 200);
	}
	public static void testMain() throws Exception { 
		Utils.testDataSample();
	}
	

}
