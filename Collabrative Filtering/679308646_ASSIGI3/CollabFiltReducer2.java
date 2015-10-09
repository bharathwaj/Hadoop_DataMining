package edu.uic.ids561.hadoop.collfilter.reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CollabFiltReducer2 extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {

	// Reducer2
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		int count = 1;
		HashMap<Integer, String> ratingsMap = new HashMap<Integer, String>();
		while (values.hasNext()) {
			ratingsMap.put(count, values.next().toString());
			count++;
		}
		double num = 0;
		double denom = 0;
		double xS2 = 0;
		double yS2 = 0;

		for (int i = 1; i <= ratingsMap.size(); i++) {
			String ratingPair = ratingsMap.get(i);
			int X = 0;
			int Y = 0;
			StringTokenizer rItr = new StringTokenizer(ratingPair, ",");
			while (rItr.hasMoreTokens()) {
				X = Integer.parseInt(rItr.nextToken());
				Y = Integer.parseInt(rItr.nextToken());
			}
			num = num + (X * Y);
			xS2 = xS2 + (X * X);
			yS2 = yS2 + (Y * Y);
		}
		denom = Math.sqrt(xS2) * Math.sqrt(yS2);

		double cosineSimilarity = num / denom;
		output.collect(key, new Text(Double.toString(cosineSimilarity)));
	}
}