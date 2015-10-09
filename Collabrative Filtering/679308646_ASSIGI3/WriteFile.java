package edu.uic.ids561.hadoop.collfilter.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

public class WriteFile {

	public static boolean storeFile(int number, JobConf job2) throws Exception {
		Map<String, Double> centHashKM = new HashMap();
		ValueComparator bvc = new ValueComparator(centHashKM);
		TreeMap<String, Double> sorted_map = new TreeMap<String, Double>(bvc);
		String path = "Output_job2/part-00000";
		System.out.println("The PAth" + path);
		Path file = new Path(path);
		FileSystem fs1 = FileSystem.get(job2);
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				fs1.open(file)));
		String currLine;
		while (true) {
			currLine = reader.readLine();
			if (currLine == null)
				break;
			String[] centstr = currLine.split("\t");
			String centKey = centstr[0];

			Double centValue = Double.parseDouble(centstr[1]);
			// System.out.println("The cent Key and value" + centKey + "<><>"
			// + centValue);
			centHashKM.put(centKey, centValue);
		}
		int count = 0;
		java.nio.file.Path pathwrite = Paths.get("output.txt");
		BufferedWriter writerBuffer = Files.newBufferedWriter(pathwrite,
				StandardCharsets.UTF_8);
		sorted_map.putAll(centHashKM);

		// System.out.println("results: " + sorted_map);
		FileSystem hdfs = FileSystem.get(job2);

		Path newFilePath = new Path("output.txt");

		hdfs.createNewFile(newFilePath);

		StringBuilder datatoLoad = new StringBuilder();

		Iterator ittemp2 = sorted_map.entrySet().iterator();
		while (ittemp2.hasNext()) {
			if (count == number)
				break;

			Map.Entry pair = (Map.Entry) ittemp2.next();
			datatoLoad.append(pair.getKey().toString() + "\t"
					+ pair.getValue().toString());
			datatoLoad.append("\n");
			count++;

		}

		byte[] byt = datatoLoad.toString().getBytes();
		FSDataOutputStream fsOutStream = hdfs.create(newFilePath);
		fsOutStream.write(byt);
		fsOutStream.close();

		return true;
	}
}
