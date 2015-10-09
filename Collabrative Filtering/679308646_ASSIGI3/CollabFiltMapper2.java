package edu.uic.ids561.hadoop.collfilter.mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class CollabFiltMapper2 extends MapReduceBase implements
		Mapper<Text, Text, Text, Text> {
	public void map(Text key, Text value, OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException {

		String processedRecord = value.toString();
		StringTokenizer itrToken = new StringTokenizer(processedRecord);

		int count1 = itrToken.countTokens();
		String[] movies = new String[count1];
		String[] ratings = new String[count1];
		int i = 0, j = 0;

		while (itrToken.hasMoreTokens()) {
			String nextTkn = itrToken.nextToken();
			StringTokenizer mrSplit = new StringTokenizer(nextTkn, ",");
			while (mrSplit.hasMoreTokens()) {
				movies[i] = mrSplit.nextToken();
				ratings[i] = mrSplit.nextToken();
				i++;
			}
		}
		for (i = 0; i < count1; i++) {
			String movie1 = movies[i];
			String rate1 = ratings[i];
			for (j = i + 1; j < count1; j++) {
				String movie2 = movies[j];
				String rate2 = ratings[j];
				int compare = movie1.compareTo(movie2);
				if (compare < 0) {
					output.collect(new Text(movie1 + "," + movie2), new Text(
							rate1 + "," + rate2));
				} else if (compare > 0) {
					output.collect(new Text(movie2 + "," + movie1), new Text(
							rate2 + "," + rate1));
				}
			}
		}
	}
}
