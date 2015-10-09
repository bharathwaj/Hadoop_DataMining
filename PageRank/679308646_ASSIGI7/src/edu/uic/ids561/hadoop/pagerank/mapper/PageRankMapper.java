package edu.uic.ids561.hadoop.pagerank.mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

//The file containts hadoop code for the mapper
public class PageRankMapper extends MapReduceBase implements
		Mapper<Text, Text, Text, Text> {
	public void map(Text key, Text value, OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException {
		// tokenizing the input for page and rank
		String page = key.toString();
		String rank_outlinks = value.toString();
		String outlinks = "";
		double rank = 0.0;
		StringTokenizer pageRankItr = new StringTokenizer(rank_outlinks);
		while (pageRankItr.hasMoreTokens()) {
			rank = new Double(pageRankItr.nextToken());
			outlinks = pageRankItr.nextToken();
		}
		StringTokenizer outlinkItr = new StringTokenizer(outlinks, ",");
		double degree = outlinkItr.countTokens();

		while (outlinkItr.hasMoreTokens()) {
			String inLink = outlinkItr.nextToken();
			double interimRank = rank / degree;
			
			output.collect(new Text(inLink), new Text(page + ":" + interimRank));
		}
		output.collect(new Text(page), new Text(outlinks));
	}

}
