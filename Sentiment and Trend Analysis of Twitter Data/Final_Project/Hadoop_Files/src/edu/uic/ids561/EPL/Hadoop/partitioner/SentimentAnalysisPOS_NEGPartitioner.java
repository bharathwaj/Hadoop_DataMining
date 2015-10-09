package edu.uic.ids561.EPL.Hadoop.partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SentimentAnalysisPOS_NEGPartitioner extends
		Partitioner<Text, IntWritable> {

	/*
	 * The Partitoner is partitions the data bases the key if its a prime it
	 * goes to one reducer and if its even and odd it goes to corresponding
	 */
	public int getPartition(Text key, IntWritable value, int reduceTaskCount) {

		if (reduceTaskCount == 0)
			return 0;
		if (key.toString().equalsIgnoreCase("postive")) {
			return 0;
		} else if (key.toString().equalsIgnoreCase("negative")) {
			return 1 % reduceTaskCount;
		} else {
			return 0;
		}

	}

}
