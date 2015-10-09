package edu.uic.ids561.EPL.Hadoop.mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.uic.ids561.EPL.Hadoop.other.FilterWords;

public class TrendAnalysisMapper extends
		Mapper<Object, Text, Text, IntWritable> {

	private Text word = new Text();
	private static final IntWritable intWritable = new IntWritable(1);

	@Override
	protected void map(Object key, Text value,
			Mapper<Object, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String textStr = "";
		textStr = FilterWords.removeStopWords(value.toString());
		StringTokenizer tokenizer = new StringTokenizer(textStr.toString());
		while (tokenizer.hasMoreTokens()) {
			String w = tokenizer.nextToken().toLowerCase();
			if ("chelseafc".contains(w) || "Chelsea".equalsIgnoreCase(w)) {
				context.write(new Text("Chelsea"), new IntWritable(1));
			}
			if ("manutd".contains(w) || "ManUtd".equalsIgnoreCase(w)) {
				context.write(new Text("ManchesterUnited"), new IntWritable(1));

			}
			if ("mcfc".contains(w) || "MCFC".equalsIgnoreCase(w)
					|| "#MCFC".equalsIgnoreCase(w)) {
				context.write(new Text("ManchesterCity"), new IntWritable(1));
			}
			if ("arsenal".contains(w)) {
				context.write(new Text("Arsenal"), new IntWritable(1));
			}
			if ("lfc".contains(w) || "LFC".equalsIgnoreCase(w)
					|| "#LFC".equalsIgnoreCase(w)) {
				context.write(new Text("Liverpool"), new IntWritable(1));
			}
			if ("spursofficial".contains(w)) {
				context.write(new Text("Tottenham"), new IntWritable(1));
			}
			if ("southamptonfc".contains(w)) {
				context.write(new Text("SouthHampton"), new IntWritable(1));
			}
			if ("swansofficial".contains(w)) {
				context.write(new Text("Swansea"), new IntWritable(1));
			}
			if ("whufc_official".contains(w)) {
				context.write(new Text("WestHamUnited"), new IntWritable(1));
			}
			if ("stokecity".contains(w)) {
				context.write(new Text("StokeCity"), new IntWritable(1));
			}

		}
	}
}
