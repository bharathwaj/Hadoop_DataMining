package edu.uic.ids561.EPL.Hadoop.reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NormSentimentAnalysisTeamReducer extends
		Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	private HashMap<String, Double> teamTweetCountMap = new HashMap<String, Double>();

	@Override
	public void setup(Context job) {
		Configuration objConfiguration = job.getConfiguration();
		String sInputPath = objConfiguration.get("pathName");
		System.out.println("NORM REDUCER __> " + sInputPath);
		BufferedReader objBufferedReader = null;
		try {
			FileStatus objFileStatus = null;
			String[] aTotalCountArray = null;
			String line = null;
			FileSystem fs = FileSystem.get(new Configuration());
			FileStatus[] status = fs.listStatus(new Path(sInputPath));
			for (int i = 0; i < status.length; i++) {
				objFileStatus = status[i];
				if (objFileStatus.getPath().getName().startsWith("part-r-00")) {
					objBufferedReader = new BufferedReader(
							new InputStreamReader(fs.open(objFileStatus
									.getPath())));
					while ((line = objBufferedReader.readLine()) != null) {
						aTotalCountArray = line.split("\t");
						teamTweetCountMap.put(aTotalCountArray[0],
								Double.parseDouble(aTotalCountArray[1]));
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				objBufferedReader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values,
			Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		double wordCount = 0;
		Iterator<DoubleWritable> it = values.iterator();
		while (it.hasNext()) {
			wordCount += it.next().get();
		}
		wordCount /= teamTweetCountMap.get(key.toString());
		// wordCount *= 100;
		context.write(key, new DoubleWritable(wordCount));
	}
}
