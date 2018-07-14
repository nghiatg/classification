package classification;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.safety.Whitelist;
import org.jsoup.select.Elements;

public class Data {
	public static HashSet<String> getSwList(String swPath) throws Exception { 
		HashSet<String> rs = new HashSet<String>();
		BufferedReader br = new BufferedReader(new FileReader(swPath));
		String line = br.readLine();
		while(line != null) {
			rs.add(line);
			line = br.readLine();
		}
		br.close();
		return rs;
	}
	
	
	//filter non_letter and non-digit here
	public static void removeSw(String input,String output) throws Exception { 
		HashSet<String> swList = getSwList("data//stopwords_connected.txt");
		PrintWriter pr = new PrintWriter(output);
		BufferedReader br = new BufferedReader(new FileReader(input));
		String line = br.readLine();
		StringBuilder sb = new StringBuilder();
		while(line != null) {
			if(!line.contains("\t")) {
				line = br.readLine();
				continue;
			}
			sb.setLength(0);
			sb.append(line.split("\t")[0]).append("\t");
			for(String word : line.split("\t")[1].split(" ")) {
				if(Utils.filterWord(word).length() == 0) {
					continue;
				}
				if(!swList.contains(Utils.filterWord(word))) {
					sb.append(Utils.filterWord(word)).append(" ");
				}
			}
			sb.setLength(sb.length()-1);
			pr.println(sb.toString());
			line = br.readLine();
		}
		br.close();
		pr.close();
	}
	
	public static void writeDataForClassification(Dataset<Row> rows,String output) throws Exception { 
		PrintWriter pr = new PrintWriter(output);
		for(Row r : rows.collectAsList()) {
			pr.println(Utils.changeRowToStrArr(r));
		}
		pr.close();
	}
	
	public static ArrayList<String> getOtherSourceData() throws Exception{
		ArrayList<String> rs = new ArrayList<String>();
		for(int i = 2 ; i < 4 ; ++i) {
			String url = "http://cafef.vn/nganh-hang-khong.html";
			Document doc = Jsoup.connect(url).get();
			for(String detailUrl : getOtherUrl(url)) {
				if(getContent(detailUrl).length() > 20) {
					System.out.println(getContent(detailUrl));
				}
			}
			break;
		}
		return rs;
	}
	
	public static String getContent(String url) throws Exception {
		Whitelist wl = Whitelist.none();
		Document doc = Jsoup.connect(url).get();
		StringBuilder sb = new StringBuilder();
		try{
			Elements eles=  doc.getElementById("mainContent").getElementsByTag("p");
			for(Element e: eles) {
				sb.append(Jsoup.clean(e.toString(), wl));
			}
		}catch(Exception e) {}
		return sb.toString();
	}
	
	public static ArrayList<String> getOtherUrl(String url) throws Exception {
		ArrayList<String> rs = new ArrayList<String>();
		Document doc = Jsoup.connect(url).get();
		Elements eles = doc.getElementsByClass("titlehidden");
		for(Element ele : eles) {
			rs.add("http://cafef.vn"+ele.getElementsByTag("a").first().attr("href"));
		}
		return rs;
	}
	
	public static void limitData(String input,String output, int limit) throws Exception { 
		HashMap<String,Integer> numberOfOccurence = new HashMap<String, Integer>();
		PrintWriter pr = new PrintWriter(output);
		BufferedReader br = new BufferedReader(new FileReader(input));
		String line = br.readLine();
		while(line != null) {
			if(!line.contains("\t")) {
				line = br.readLine();
				continue;
			}
			if(!numberOfOccurence.containsKey(line.split("\t")[0])) {
				numberOfOccurence.put(line.split("\t")[0], 0);
			}
			if(numberOfOccurence.get(line.split("\t")[0]) >= limit) {
				line = br.readLine();
				continue;
			}
			numberOfOccurence.put(line.split("\t")[0], numberOfOccurence.get(line.split("\t")[0]) + 1);
			pr.println(line);
			line = br.readLine();
		}
		br.close();
		pr.close();
	}
	
	public static String crawl(String url) throws Exception { 
		StringBuilder sb = new StringBuilder();
		Document doc = Jsoup.connect(url).get();
		Elements pEles = doc.getElementsByTag("p");
		Whitelist wl = Whitelist.none();
		for(Element e : pEles) {
			sb.append(Jsoup.clean(e.toString(), wl)).append(" ");
		}
		return sb.toString();
	}
	
	public static ArrayList<String> getTest() throws Exception { 
		ArrayList<String> rs= new ArrayList<String>();
		BufferedReader br = new BufferedReader(new FileReader("D:\\Esclipse\\eclipseproject\\classification\\data\\test2.txt"));
		String line = br.readLine();
		while(line != null) {
			rs.add(line);
			line = br.readLine();
		}
		br.close();
		return rs;
	}

}
