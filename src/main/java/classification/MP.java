package classification;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class MP {

	public static void main(String[] args) throws Exception {
		ML.spark.sparkContext().setLogLevel("ERROR");
		Dataset<Row> rawDataIntoDataset = ML.createDataSet();
		Dataset<Row> afterCV = ML.cv(rawDataIntoDataset,"vocabulary");
		Dataset<Row> tfidf = ML.tfidf(afterCV);
		tfidf.show(false);

	}
	

}
