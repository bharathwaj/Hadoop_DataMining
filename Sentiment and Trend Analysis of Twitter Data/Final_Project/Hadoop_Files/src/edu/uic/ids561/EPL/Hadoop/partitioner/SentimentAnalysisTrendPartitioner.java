package edu.uic.ids561.EPL.Hadoop.partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SentimentAnalysisTrendPartitioner extends
		Partitioner<Text, IntWritable> {

	/*
	 * The Partitoner is partitions the data bases the key if its a prime it
	 * goes to one reducer and if its even and odd it goes to corresponding
	 */
	public int getPartition(Text key, IntWritable value, int reduceTaskCount) {

		if (reduceTaskCount == 0)
			return 0;
		if (key.toString().equalsIgnoreCase("Man​chester​United")) {
			return 0;
		} else if (key.toString().equalsIgnoreCase("Chelsea")) {
			return 1 % reduceTaskCount;
		} else if (key.toString().equalsIgnoreCase("Man​chester​City")) {
			return 2 % reduceTaskCount;
		} else if (key.toString().equalsIgnoreCase("Arsenal")) {
			return 3 % reduceTaskCount;
		} else if (key.toString().equalsIgnoreCase("Liverpool")) {
			return 4 % reduceTaskCount;
		} else if (key.toString().equalsIgnoreCase("Tottenham")) {
			return 5 % reduceTaskCount;
		} else if (key.toString().equalsIgnoreCase("SouthHampton")) {
			return 6 % reduceTaskCount;
		} else if (key.toString().equalsIgnoreCase("Swansea")) {
			return 7 % reduceTaskCount;
		} else if (key.toString().equalsIgnoreCase("WestHamU​nited")) {
			return 8 % reduceTaskCount;
		} else if (key.toString().equalsIgnoreCase("StokeCity")) {
			return 9 % reduceTaskCount;
		} else {
			return 0;
		}

	}

}
