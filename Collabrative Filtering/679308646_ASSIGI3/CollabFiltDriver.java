package edu.uic.ids561.hadoop.collfilter.driver;

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

import edu.uic.ids561.hadoop.collfilter.mapper.CollabFiltMapper1;
import edu.uic.ids561.hadoop.collfilter.mapper.CollabFiltMapper2;
import edu.uic.ids561.hadoop.collfilter.reducer.CollabFiltReducer1;
import edu.uic.ids561.hadoop.collfilter.reducer.CollabFiltReducer2;
import edu.uic.ids561.hadoop.collfilter.util.WriteFile;

public class CollabFiltDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		// creating Jobs job1 and job2 with JobConf reference
		JobConf job1 = new JobConf(getConf(), CollabFiltDriver.class);
		job1.setJobName("job1");

		JobConf job2 = new JobConf(getConf(), CollabFiltDriver.class);
		job2.setJobName("job2");

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		job1.setJarByClass(CollabFiltDriver.class);
		job1.setMapperClass(CollabFiltMapper1.class);
		job1.setNumReduceTasks(1);
		job1.setReducerClass(CollabFiltReducer1.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setMapperClass(CollabFiltMapper2.class);
		job2.setReducerClass(CollabFiltReducer2.class);

		job1.setInputFormat(KeyValueTextInputFormat.class);
		KeyValueTextInputFormat.addInputPath(job1, new Path("input"));
		FileOutputFormat.setOutputPath(job1, new Path("Output_job1"));

		// Running Job1
		JobClient.runJob(job1);

		job2.setInputFormat(KeyValueTextInputFormat.class);
		KeyValueTextInputFormat.addInputPath(job2, new Path("Output_job1"));
		FileOutputFormat.setOutputPath(job2, new Path("Output_job2"));

		JobClient.runJob(job2);
		// Code for reading the file change the value to get the number of lines 
		WriteFile.storeFile(100, job2);
		return 0;

	}

	public static void main(String[] args) throws Exception {

		// Main class
		int result = ToolRunner.run(new Configuration(),
				new CollabFiltDriver(), args);
		System.exit(result);

	}
}
