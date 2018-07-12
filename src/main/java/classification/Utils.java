package classification;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class Utils {
	
	public static String filterWord(String raw) {
		int start = 0;
		int end = raw.length()-1;
		while(!Character.isLetterOrDigit(raw.charAt(start)) && start < raw.length() && raw.charAt(start) != '_') {
			start++;
		}
		while(!Character.isLetterOrDigit(raw.charAt(end)) && end > -1 && raw.charAt(start) != '_') {
			end--;
		}
		if(end > start) {
			return raw.substring(start, end+1);
		}else {
			return "";
		}
	}
	
	public static Row changeStringToRow(String doc, int id,int label) throws IOException {
		ArrayList<String> words = new ArrayList<String>();
		for(String w : doc.split(" ")) {
			words.add(w);
		}
		Row r = RowFactory.create(new Long(id),new Integer(label), words);
		return r;
	}
	
	public static void writeVocab(String[] vocab, String output) throws Exception {
		PrintWriter pr = new PrintWriter(output);
		for(int i = 0 ; i < vocab.length ; ++i) {
			pr.println(vocab[i]);
		}
		pr.close();
	}


}
