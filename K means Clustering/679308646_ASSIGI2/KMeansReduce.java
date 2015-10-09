package edu.uic.ids561.hadoop.kmeans;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//import edu.uic.ids561.hadoop.kmeans.KMeansJob.KMeansReduce;

public class KMeansReduce extends Reducer<Text, Text, Text, Text> {
	static double[] centCalc;

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		centCalc = new double[10];
		double[] finalcentCalc = new double[10];
		String finalval = new String();
		int count = 0;
		LongWritable cent = new LongWritable();
		for (Text value : values) {
			count++;
			String[] Valuestr = value.toString().split(",");
			KMeansReduce.calculateCent(Valuestr);
		}
		for (int j = 0; j < 10; j++) {
			finalcentCalc[j] = centCalc[j] / count;
			finalcentCalc[j] = Math.round(finalcentCalc[j] * 100.0) / 100.0;
			finalval = finalval + String.valueOf(finalcentCalc[j]);
			if (j != 9) {
				finalval = finalval + ",";
			}
		}
		context.write(key, new Text(finalval));
	}

	public static String[] calculateCent(String[] value) {
		for (int i = 0; i < 10; i++) {
			centCalc[i] = centCalc[i] + Double.parseDouble(value[i]);
		}
		return value;
	}
}
