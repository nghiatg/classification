package classification;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.HashSet;

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
		BufferedReader br = new BufferedReader(new FileReader("datatrain_more_pped.txt"));
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

}
