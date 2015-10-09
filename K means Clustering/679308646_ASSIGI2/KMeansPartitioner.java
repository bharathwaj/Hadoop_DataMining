package edu.uic.ids561.hadoop.kmeans;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class KMeansPartitioner extends Partitioner<Text, Text> {

	/*
	 * The Partitoner is partitions the data bases the key if its a prime it
	 * goes to one reducer and if its even and odd it goes to corresponding
	 */
	public int getPartition(Text key, Text value, int reduceTaskCount) {

		if (reduceTaskCount == 0)
			return 0;

		if (key.toString().equalsIgnoreCase("C1")) {
			return 0;
		} else if (key.toString().equalsIgnoreCase("C2")) {
			return 1 % reduceTaskCount;
		} else {
			return 2 % reduceTaskCount;
		}
	}

}
