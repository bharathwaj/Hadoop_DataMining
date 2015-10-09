package edu.uic.ids561.EPL.Hadoop.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.uic.ids561.EPL.Hadoop.mapper.NormSentimentAnalysisTeamMapper;
import edu.uic.ids561.EPL.Hadoop.mapper.SentimentAnalysisMapper;
import edu.uic.ids561.EPL.Hadoop.mapper.TrendAnalysisMapper;
import edu.uic.ids561.EPL.Hadoop.other.FileUtil;
import edu.uic.ids561.EPL.Hadoop.partitioner.SentimentAnalysisPOS_NEGPartitioner;
import edu.uic.ids561.EPL.Hadoop.partitioner.SentimentAnalysisTrendPartitioner;
import edu.uic.ids561.EPL.Hadoop.reducer.NormSentimentAnalysisTeamReducer;
import edu.uic.ids561.EPL.Hadoop.reducer.SentimentAnalysisReducer;
import edu.uic.ids561.EPL.Hadoop.reducer.TrendAnalysisReducer;

public class SentimentAnalysis extends Configured implements Tool {
	public int run(String[] args) throws Exception {

		FileSystem fs = FileSystem.get(new Configuration());
		FileStatus[] aFileStatus = fs.listStatus(new Path(args[0]));
		String input, output, sOutputTrend, sPathName;
		FileUtil objFileUtil = new FileUtil();
		for (int i = 0; i < aFileStatus.length; i++) {
			sPathName = aFileStatus[i].getPath().getName();
			if (aFileStatus[i].isDir()) {
				if ((args[0] + "/BPL").contains(sPathName)) {
					Configuration conf = new Configuration();
					Job job = new Job(conf, "SAD");
					job.setJarByClass(SentimentAnalysis.class);
					job.setMapperClass(TrendAnalysisMapper.class);
					job.setPartitionerClass(SentimentAnalysisTrendPartitioner.class);
					job.setReducerClass(TrendAnalysisReducer.class);
					job.setNumReduceTasks(10);
					job.setOutputKeyClass(Text.class);
					job.setOutputValueClass(IntWritable.class);
					input = args[0];
					sOutputTrend = args[1] + "_TREND";

					FileInputFormat.setInputPaths(job, new Path(args[0]
							+ "/BPL"));
					/* Setting the values classes for the mapper reducer */
					FileOutputFormat.setOutputPath(job, new Path(sOutputTrend));
					job.waitForCompletion(true);
					objFileUtil.processOutPutFiles(conf, sOutputTrend, args[2]
							+ "/output_TREND", "Team", "Count");
					break;
				}
			}
		}
		for (int i = 0; i < aFileStatus.length; i++) {
			sPathName = aFileStatus[i].getPath().getName();
			if (aFileStatus[i].isDir()) {
				if ((args[0] + "/BPL").contains(sPathName)) {
				} else {
					Configuration conf = new Configuration();
					Job job = new Job(conf, "SA");
					job.setJarByClass(SentimentAnalysis.class);
					job.setMapperClass(SentimentAnalysisMapper.class);
					job.setReducerClass(SentimentAnalysisReducer.class);
					job.setPartitionerClass(SentimentAnalysisPOS_NEGPartitioner.class);
					job.setNumReduceTasks(2);
					job.setOutputKeyClass(Text.class);
					job.setOutputValueClass(IntWritable.class);
					input = args[0] + "/" + sPathName;
					output = args[1] + "_SENTI/" + sPathName;

					FileInputFormat.setInputPaths(job, new Path(input));
					/* Setting the values classes for the mapper reducer */
					FileOutputFormat.setOutputPath(job, new Path(output));
					job.waitForCompletion(true);
					objFileUtil.processOutPutFiles(conf, output, args[2]
							+ "/output_SENTI/" + sPathName, "Sentiment",
							"Count");

					/** Change new mapper and reducer. */
					Configuration conf1 = new Configuration();
					output = args[1] + "_NORM/" + sPathName;
					conf1.set("folderName", sPathName);
					conf1.set("pathName", args[1] + "_TREND");
					Job job1 = new Job(conf1, "Norm");
					job1.setJarByClass(SentimentAnalysis.class);
					job1.setMapperClass(NormSentimentAnalysisTeamMapper.class);
					job1.setReducerClass(NormSentimentAnalysisTeamReducer.class);
					job1.setNumReduceTasks(1);
					job1.setOutputKeyClass(Text.class);
					job1.setOutputValueClass(DoubleWritable.class);
					FileInputFormat.setInputPaths(job1, new Path(input));
					FileOutputFormat.setOutputPath(job1, new Path(output));
					job1.waitForCompletion(true);
					objFileUtil.copyFromHadoopToLocal(conf1, output, args[2]
							+ "/output_NORM/" + sPathName);
				}
			}
		}
		objFileUtil.processOutPutFilesForRatings(args[2] + "/output_NORM/",
				"Team", "Rating");
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new SentimentAnalysis(),
				args);
	}
}
