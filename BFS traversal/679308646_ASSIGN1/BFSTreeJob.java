package edu.uic.ids561.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BFSTreeJob extends Configured implements Tool {

	public static class BFSMap extends BFSTreeMapper {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			Node inNode = new Node(value.toString());
			super.map(key, value, context, inNode);

		}
	}

	public static class BFSReduce extends BFSTreeReducer {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			Node outNode = new Node();
			outNode = super.reduce(key, values, context, outNode);

		}
	}

	public int run(String[] args) throws Exception {

		int iterationCount = 0;

		while (Counter.Conter > 0) {
			/*Setting the values classes for the mapper reducer */
			Job job = new Job(new Configuration(), "bfstreejob");
			job.setJarByClass(BFSTreeJob.class);
			job.setMapperClass(BFSMap.class);
			job.setReducerClass(BFSReduce.class);
			job.setPartitionerClass(BFSTreePartitioner.class);
			job.setNumReduceTasks(3);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			String input, output;
			if (iterationCount == 0)
				input = args[0];
			else
				input = args[1] + iterationCount;
			Counter.sourceNode = Long.parseLong(args[2]);
			output = args[1] + (iterationCount + 1);
			FileInputFormat.setInputPaths(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
			job.waitForCompletion(true);
			Counters jobCntrs = job.getCounters();
			iterationCount++;
			Counter.jobConter = iterationCount;
		}

		return 0;
	}

	public static void main(String[] args) throws Exception {
		
		  GraphStats.generatStats(args); /*System.exit(0);*/
		 
		int res = ToolRunner.run(new Configuration(), new BFSTreeJob(), args);

		System.exit(res);
	}

}