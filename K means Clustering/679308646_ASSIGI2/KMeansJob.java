package edu.uic.ids561.hadoop.kmeans;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class KMeansJob extends Configured implements Tool {

	public static HashMap<String, String> centHashKM = new HashMap();

	public int run(String[] args) throws Exception {

		int iterationCount = 0;
		Configuration conf = new Configuration();
		conf.set("iterationCount", String.valueOf(iterationCount));
		/* Setting the values classes for the mapper reducer */
		while (true) {

			Job job = new Job(conf, "Kmeans");
			job.setJarByClass(KMeansJob.class);
			job.setMapperClass(KMeansMap.class);
			job.setReducerClass(KMeansReduce.class);
			job.setPartitionerClass(KMeansPartitioner.class);
			job.setNumReduceTasks(1);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			String input, output;
			input = args[0];
			output = args[2] + (iterationCount + 1);
			FileInputFormat.setInputPaths(job, new Path(input));
			// Commands to read the file from the centroid folder
			File folder = null, folderback = null;
			HashMap temphash = new HashMap<String, String>();
			centHashKM = new HashMap();
			String path;
			if (iterationCount == 0) {
				path = "K_Means_II/output/centroid.txt";
			} else {
				path = "K_Means_II/output" + iterationCount + "/part-r-00000";
			}
			System.out.println("The PAth" + path);
			Path prevfile = new Path(path);
			FileSystem fs1 = FileSystem.get(conf);
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
				centHashKM.put(centKey, centValue);
			}

			String pathRev = null;
			if (iterationCount != 0) {
				if (iterationCount == 1) {
					pathRev = "K_Means_II/output/centroid.txt";
				} else {
					pathRev = "K_Means_II/output" + (iterationCount - 1)
							+ "/part-r-00000";
				}
			}
			if (iterationCount > 0) {
				Path prevfileCent = new Path(pathRev);
				System.out.println("The PAth After " + prevfileCent);
				FileSystem fs12 = FileSystem.get(conf);
				BufferedReader reader2 = new BufferedReader(
						new InputStreamReader(fs12.open(prevfileCent)));
				String currLineb;
				while (true) {
					currLineb = reader2.readLine();
					if (currLineb == null)
						break;
					String[] centstrb = currLineb.split("\t");
					String centKeyb = centstrb[0];
					String centValueb = centstrb[1];
					temphash.put(centKeyb, centValueb);
				}
			}

			iterationCount++;

			FileOutputFormat.setOutputPath(job, new Path(output));
			job.waitForCompletion(true);
			Iterator it = centHashKM.entrySet().iterator();
			int counti = 0, countj = 0;
			String[] strIn = new String[3];
			while (it.hasNext()) {
				Map.Entry Centpair = (Map.Entry) it.next();
				strIn[counti] = Centpair.getValue().toString();
				counti++;
			}

			Iterator ittemp2 = temphash.entrySet().iterator();
			String[] strTemp = new String[3];
			while (ittemp2.hasNext()) {
				Map.Entry pair = (Map.Entry) ittemp2.next();
				strTemp[countj] = pair.getValue().toString();
				countj++;
			}

			double DisttoExit = 0;
			if (iterationCount > 1) {
				for (int a = 0; a < centHashKM.size(); a++) {
					String[] st = strIn[a].split(",");
					String[] std = strTemp[a].split(",");
					DisttoExit = DisttoExit + calculateDist(st, std);
				}
				if (DisttoExit < 2)
					break;
			}
		}

		return 0;
	}

	public double calculateDist(String[] prevHash, String[] currHash) {
		double[] minDist = new double[centHashKM.size()];
		double minD = 0;

		for (int i = 0; i < centHashKM.size(); i++) {
			for (int id = 0; id < 10; id++) {
				minDist[i] = minDist[i]
						+ Math.pow(
								Double.parseDouble(prevHash[id])
										- Double.parseDouble(currHash[id]), 2);
			}
			minDist[i] = Math.sqrt(minDist[i]);
			minD = minD + minDist[i];

		}
		return minD;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new KMeansJob(), args);
		System.exit(res);
	}

}