package edu.uic.ids561.hadoop.pagerank.reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class PageRankReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		double rankFinal = 0.0;
		double beta = 0.85;
		String outLinks = "";
		// iterate through all Item sets and sum identical sets
		int count = 0;
		LongWritable cent = new LongWritable();
		// Iterator<Text> newVak = new Iterator<Text>();

		while (values.hasNext()) {
			String inLink = "";

			String nextValue = values.next().toString();
			if (!(nextValue.indexOf(":") == -1)) {

				StringTokenizer rankItr = new StringTokenizer(nextValue, ":");
				while (rankItr.hasMoreTokens()) {
					inLink = rankItr.nextToken();
					rankFinal = rankFinal
							+ (new Double(rankItr.nextToken()) * beta);
				}

			} else {
				outLinks = nextValue;
			}
			count++;
		}
		rankFinal += 0.15 / count;
		output.collect(
				new Text(key + "	" + (new Double(rankFinal).toString())),
				new Text(outLinks));
	}
}