package edu.uic.ids561.hadoop.kmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KMeansMap extends Mapper<Object, Text, Text, Text> {
	static HashMap<String, String> centHash = new HashMap();

	public void setup(Context context) throws IOException {

		Configuration conf = context.getConfiguration();
		String param = conf.get("iterationCount");
		// int counterDis = context.getJobID().toString().length();
		int iterationCount = Integer.parseInt(param);
		String path;
		if (iterationCount == 0) {
			path = "K_Means_II/output/centroid.txt";
		} else {
			path = "K_Means_II/output" + iterationCount + "/part-r-00000";
		}
		System.out.println("The path in the Reducer" + path);
		Path prevfile = new Path(path);
		FileSystem fs1 = FileSystem.get(new Configuration());
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				fs1.open(prevfile)));
		String currLine;
		while (true) {
			currLine = reader.readLine();
			if (currLine == null)
				break;
			String[] centstr = currLine.split("\t");
			String centKey = centstr[0];
			String centValue = centstr[1];
			System.out.println(centValue);
			centHash.put(centKey, centValue);
		}

	}

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		HashMap centHash = KMeansMap.centHash;
		String[] pieces = value.toString().split("\\t");
		key = pieces[0].trim();
		double[] minDist = new double[centHash.size()];
		value.set(pieces[1].trim());
		// System.out.println("The value in the mapper" + pieces[1].trim());
		String[] Valuestr = value.toString().split(",");

		Iterator it = centHash.entrySet().iterator();

		while (it.hasNext()) {
			Map.Entry Centpair = (Map.Entry) it.next();
			String[] centstr = Centpair.getValue().toString().split(",");
			int j = Integer.parseInt(Centpair.getKey().toString().substring(1)) - 1;
			for (int id = 0; id < 10; id++) {
				minDist[j] = minDist[j]

						+ Math.pow(
								Double.parseDouble(centstr[id])
										- Double.parseDouble(Valuestr[id]), 2);
			}
			minDist[j] = Math.sqrt(minDist[j]);
		}
		double smallest = minDist[0];
		for (int i = 1; i < minDist.length; i++) {
			if (minDist[i] < smallest)
				smallest = minDist[i];
		}
		for (int i = 0; i < minDist.length; i++)
			if (minDist[i] == smallest) {
				int k = i + 1;
				context.write(new Text("C" + String.valueOf(k)), value);
			}

	}
}
