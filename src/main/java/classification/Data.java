package classification;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.safety.Whitelist;
import org.jsoup.select.Elements;

public class Data {
	public static HashSet<String> getSwList() throws Exception { 
		HashSet<String> rs = new HashSet<String>();
		BufferedReader br = new BufferedReader(new FileReader("stopwords_connected.txt"));
		String line = br.readLine();
		while(line != null) {
			rs.add(line);
			line = br.readLine();
		}
		br.close();
		return rs;
	}
	
	
	//filter non_letter and non-digit here
	public static void removeSw() throws Exception { 
		HashSet<String> swList = getSwList();
		PrintWriter pr = new PrintWriter("data_nosw.txt");
		BufferedReader br = new BufferedReader(new FileReader("datatrain_more_pped_changeLabel.txt"));
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

}
