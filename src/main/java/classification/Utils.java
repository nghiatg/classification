package classification;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.apache.spark.sql.Dataset;
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
	
	public static Row changeStringToRowWithLabel(int id,int label,String doc) throws IOException {
		ArrayList<String> words = new ArrayList<String>();
		for(String w : doc.split(" ")) {
			words.add(w);
		}
		Row r = RowFactory.create(new Long(id),new Integer(label), words);
		return r;
	}
	
	public static Row changeStringToRowWithNoLabel(int id, String doc) throws IOException {
		ArrayList<String> words = new ArrayList<String>();
		for(String w : doc.split(" ")) {
			words.add(w);
		}
		Row r = RowFactory.create(new Long(id), words);
		return r;
	}
	
	public static void writeVocab(String[] vocab, String output) throws Exception {
		PrintWriter pr = new PrintWriter(output);
		for(int i = 0 ; i < vocab.length ; ++i) {
			pr.println(vocab[i]);
		}
		pr.close();
	}
	
	public static void testDataSample() throws Exception { 
		Dataset<Row> data = ML.spark.read().format("libsvm").load("data_sample.txt");
		data.printSchema();
		data.show();

	}
	
	public static String changeRowToStrArr(Row r) throws Exception { 
		StringBuilder sb = new StringBuilder();
		String strRow = r.toString();
		String[] eles = strRow.split("\\[");
		String[] index = eles[2].split("\\]")[0].split(",");
		String[] values = eles[3].split("\\]")[0].split(",");
		double[] tfidfArr = new double[ML.vocabLength];

		sb.append(strRow.substring(1, strRow.indexOf(",")));
		for(int i=0 ; i < index.length ; ++i) {
			tfidfArr[Integer.parseInt(index[i])] = Double.parseDouble(values[i]);
		}
		for(int i=0 ; i < ML.vocabLength;++i) {
			if(tfidfArr[i] == 0) {
				continue;
			}
			sb.append(" ").append(i+1).append(":").append(tfidfArr[i]);
		}
		return sb.toString();
	}
	
	public static int getVocabLength() throws Exception { 
		int rs = 0;
		BufferedReader br = new BufferedReader(new FileReader("data//vocabulary"));
		String line = br.readLine();
		while(line != null) {
			rs++;
			line = br.readLine();
		}
		br.close();
		return rs;
	}
	
//	public static void shuffle(String ip, String op) throws Exception { 
//		ArrayList<String> data = new ArrayList<String>();
//		BufferedReader br = new BufferedReader(new FileReader(ip));
//		String line = br.readLine();
//		while(line != null) {
//			data.add(line);
//			line = br.readLine();
//		}
//		br.close();
//		Collections.shuffle(data);
//		PrintWriter pr = new PrintWriter(op);
//		for(String s : data) {
//			pr.println(s);
//		}
//		pr.close();
//	}
	
	public static void changeLabel() throws Exception {
		PrintWriter pr = new PrintWriter("data//datatrain_more_pped_changeLabel.txt");
		
		//key : old, value : new
		HashMap<Integer,Integer> labels = new HashMap<Integer,Integer>();
		labels.put(1,0);
		labels.put(2,1);
		for(int i = 6 ; i < 25 ; ++i) {
			labels.put(i,i-4);
		}
		labels.put(156,21);
		labels.put(188,22);
		BufferedReader br = new BufferedReader(new FileReader("data//datatrain_more_pped.txt"));
		String line  = br.readLine();
		while(line != null) {
			if(!line.contains("\t")) {
				pr.println(line);
				line = br.readLine();
				continue;
			}
			
			pr.println(labels.get(Integer.parseInt(line.split("\t")[0])) + line.substring(line.indexOf('\t')));
			line = br.readLine();
		}
		br.close();
		pr.close();
	}
	
	public static void getLackData() throws Exception {
		BufferedReader br = new BufferedReader(new FileReader("data//datatrain_more_pped_changeLabel.txt"));
		String line = br.readLine();
		while(line != null) {
			if(line.startsWith("1\t") || line.startsWith("14") || line.startsWith("21") || line.startsWith("18")) {
				System.out.println(line);
			}
			line = br.readLine();
		}
		br.close();
		
	}
	
	public static String formatDouble(double d, int numberAfterComma) {
		DecimalFormat df = new DecimalFormat("#");
		df.setMaximumFractionDigits(numberAfterComma);
		return (df.format(d)); 
	}
	
	public static void splitFile(String input, double trainRatio, String trainPath, String testPath) throws Exception {
		BufferedReader br = new BufferedReader(new FileReader(input));
		PrintWriter prTrain = new PrintWriter(trainPath);
		PrintWriter prTest = new PrintWriter(testPath);
		String line = br.readLine();
		HashMap<String,ArrayList<String>> count = new HashMap<String, ArrayList<String>>();
		while(line != null) {
			if(!count.containsKey(line.split("\t")[0])) {
				count.put(line.split("\t")[0],new ArrayList<String>());
			}
			count.get(line.split("\t")[0]).add(line);
			line = br.readLine();
		}
		
		br.close();
	}
	
	public static void deleteDir(File file) {
	    File[] contents = file.listFiles();
	    if (contents != null) {
	        for (File f : contents) {
	            deleteDir(f);
	        }
	    }
	    file.delete();
	}


}
