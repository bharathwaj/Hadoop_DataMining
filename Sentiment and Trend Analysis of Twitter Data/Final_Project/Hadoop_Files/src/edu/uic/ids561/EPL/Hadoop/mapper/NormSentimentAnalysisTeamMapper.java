package edu.uic.ids561.EPL.Hadoop.mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.uic.ids561.EPL.Hadoop.other.FilterWords;
import edu.uic.ids561.EPL.Hadoop.other.ResourceLoader;

public class NormSentimentAnalysisTeamMapper extends
		Mapper<Object, Text, Text, DoubleWritable> {

	private static ResourceLoader loader = null;
	private static String sTeamName = null;

	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		sTeamName = conf.get("folderName");
		loader = ResourceLoader.getInstance(conf);

	}

	@Override
	protected void map(Object key, Text value,
			Mapper<Object, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		String textStr = FilterWords.removeStopWords(value.toString());
		StringTokenizer tokenizer = new StringTokenizer(textStr.toString());
		Text word = new Text(sTeamName);
		while (tokenizer.hasMoreTokens()) {
			String w = tokenizer.nextToken().toLowerCase();
			if (loader.getPostiveWords().contains(w)) {
				context.write(word, new DoubleWritable(1));
			} else if (loader.getNegativeWords().contains(w)) {
				context.write(word, new DoubleWritable(-1));
			}
		}
	}
}
