package edu.uic.ids561.hadoop.collfilter.mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class CollabFiltMapper1 extends MapReduceBase implements
		Mapper<Text, Text, Text, Text> {

	public void map(Text key, Text value, OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException {
		Text movieRating = new Text();
		String record = value.toString();
		StringTokenizer itrToken = new StringTokenizer(record);
		while (itrToken.hasMoreTokens()) {
			movieRating.set((itrToken.nextToken()) + ","
					+ (itrToken.nextToken()));
			itrToken.nextToken();
			output.collect(key, movieRating);
		}
	}

}
