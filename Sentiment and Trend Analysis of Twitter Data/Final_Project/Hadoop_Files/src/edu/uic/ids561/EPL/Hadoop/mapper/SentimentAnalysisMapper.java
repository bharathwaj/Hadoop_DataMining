package edu.uic.ids561.EPL.Hadoop.mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.uic.ids561.EPL.Hadoop.other.FilterWords;
import edu.uic.ids561.EPL.Hadoop.other.ResourceLoader;

public class SentimentAnalysisMapper extends
		Mapper<Object, Text, Text, IntWritable> {

	private static ResourceLoader loader = null;

	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		loader = ResourceLoader.getInstance(conf);
	}

	@Override
	protected void map(Object key, Text value,
			Mapper<Object, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String textStr = FilterWords.removeStopWords(value.toString());
		StringTokenizer tokenizer = new StringTokenizer(textStr.toString());
		Text word = new Text();
		while (tokenizer.hasMoreTokens()) {
			String w = tokenizer.nextToken().toLowerCase();
			if (loader.getPostiveWords().contains(w)) {
				word.set("postive");
				context.write(word, new IntWritable(1));
			} else if (loader.getNegativeWords().contains(w)) {
				word.set("negative");
				context.write(word, new IntWritable(1));
			}
		}
	}
}
