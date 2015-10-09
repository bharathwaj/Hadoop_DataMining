package edu.uic.ids561.EPL.Hadoop.other;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ResourceLoader {

	private Set<String> postiveWords = new HashSet<String>();
	private Set<String> negativeWords = new HashSet<String>();
	private Configuration conf = null;

	public static ResourceLoader instance = null;

	private ResourceLoader(Configuration conf) {
		this.conf = conf;
		loadPostive();
		loadNegative();
	}

	private void loadNegative() {
		BufferedReader objBufferedReader = null;
		try {
			FileSystem fs = FileSystem.get(conf);
			objBufferedReader = new BufferedReader(new InputStreamReader(
					fs.open(new Path("neg-words.txt"))));
			String line;
			while ((line = objBufferedReader.readLine()) != null) {
				negativeWords.add(line.toLowerCase());
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

	private void loadPostive() {
		BufferedReader objBufferedReader = null;
		try {
			FileSystem fs = FileSystem.get(conf);
			objBufferedReader = new BufferedReader(new InputStreamReader(
					fs.open(new Path("pos-words.txt"))));
			String line;
			while ((line = objBufferedReader.readLine()) != null) {
				postiveWords.add(line.toLowerCase());
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

	public static ResourceLoader getInstance(Configuration conf) {
		if (instance == null) {
			instance = new ResourceLoader(conf);
			instance.conf = conf;
		}
		return instance;
	}

	public Set<String> getPostiveWords() {
		return postiveWords;
	}

	public Set<String> getNegativeWords() {
		return negativeWords;
	}

}
