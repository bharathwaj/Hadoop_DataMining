package edu.uic.ids561.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class BFSTreePartitioner extends Partitioner<Text, Text> {

	/*
	 * The Partitoner is partitions the data bases the key if its a prime it
	 * goes to one reducer and if its even and odd it goes to corresponding
	 */
	public int getPartition(Text key, Text value, int reduceTaskCount) {

		if (reduceTaskCount == 0)
			return 0;
		int iddr = Integer.parseInt(key.toString());
		if (BFSTreePartitioner.isPrime(iddr)) {
			return 0;
		} else if (iddr % 2 == 0) {
			return 1 % reduceTaskCount;
		} else {
			return 2 % reduceTaskCount;
		}
	}

	public static boolean isPrime(int n) {
		if (n <= 3) {
			return n > 1;
		} else if (n % 2 == 0 || n % 3 == 0) {
			return false;
		} else {
			for (int i = 5; i * i <= n; i += 6) {
				if (n % i == 0 || n % (i + 2) == 0) {
					return false;
				}
			}
			return true;
		}
	}

}
