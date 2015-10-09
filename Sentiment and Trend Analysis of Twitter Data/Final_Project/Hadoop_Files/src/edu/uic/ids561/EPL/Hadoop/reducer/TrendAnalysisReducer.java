package edu.uic.ids561.EPL.Hadoop.reducer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class TrendAnalysisReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable totalWordCount = new IntWritable();

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		int wordCount = 0;
		Iterator<IntWritable> it = values.iterator();
		while (it.hasNext()) {
			wordCount += it.next().get();
		}
		totalWordCount.set(wordCount);
		context.write(key, totalWordCount);
	}

}
