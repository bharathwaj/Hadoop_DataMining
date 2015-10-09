package edu.uic.ids561.hadoop.collfilter.reducer;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CollabFiltReducer1 extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		// Input variable declaration and initiation
		String movRating = "";
		HashMap<String, String> movieRateMap = new HashMap<String, String>();
		// Append brand_TS associated with each UID
		while (values.hasNext()) {
			StringTokenizer mvRateSplit = new StringTokenizer(values.next()
					.toString(), ",");
			movieRateMap.put(mvRateSplit.nextToken(), mvRateSplit.nextToken());
		}
		// SORT, FORM LIST and LOAD
		// SORT
		Set<String> movieSet = movieRateMap.keySet();
		ArrayList<String> sortedList = new ArrayList<>(movieSet);
		Collections.sort(sortedList);
		Iterator<String> movieItr = sortedList.iterator();
		// FORM LIST
		while (movieItr.hasNext() == true) {
			String movie = movieItr.next();
			String rating = movieRateMap.get(movie);
			movRating = movRating.concat(movie.concat(",").concat(rating))
					.concat(" ");
		}
		// LOAD - Output collector - Reducer1
		output.collect(key, new Text(movRating));
	}
}
// movie, array list sort sorted list.get(1)