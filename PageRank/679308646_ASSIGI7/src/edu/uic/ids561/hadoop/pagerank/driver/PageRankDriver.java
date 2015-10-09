package edu.uic.ids561.hadoop.pagerank.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.uic.ids561.hadoop.pagerank.mapper.PageRankMapper;
import edu.uic.ids561.hadoop.pagerank.reducer.PageRankReducer;

public class PageRankDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		// creating Jobs prjb1 with JobConf reference

		int maxIterations = 100;
		for (int i = 1; i <= maxIterations; i++) {

			JobConf prjb1 = new JobConf(getConf(), PageRankDriver.class);
			prjb1.setJobName("Jb1");

			prjb1.setOutputKeyClass(Text.class);
			prjb1.setOutputValueClass(Text.class);
			prjb1.setJarByClass(PageRankDriver.class);
			prjb1.setMapperClass(PageRankMapper.class);
			prjb1.setNumReduceTasks(1);
			prjb1.setReducerClass(PageRankReducer.class);
			prjb1.setInputFormat(KeyValueTextInputFormat.class);
			if (i == 1) {
				// input directory to be referred to from the command line
				KeyValueTextInputFormat.addInputPath(prjb1, new Path("Input1"));
			} else {
				// input directory to be referred to from the command line
				KeyValueTextInputFormat.addInputPath(prjb1, new Path(
						"Output_JOB" + (i - 1)));
			}
			// output directory for the output
			FileOutputFormat.setOutputPath(prjb1, new Path("Output_JOB" + i));
			// Running Job1
			JobClient.runJob(prjb1);

		}
		return 0;
	}

	public static void main(String[] args) throws Exception {

		int result = ToolRunner.run(new Configuration(), new PageRankDriver(),
				args);
		System.exit(result);

	}
}
