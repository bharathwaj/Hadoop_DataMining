package edu.uic.ids561.EPL.Hadoop.other;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileUtil {

	public void copyFromHadoopToLocal(Configuration objConfiguration,
			String sHadoopInputpath, String sLocalOutputPath)
			throws IOException {
		FileSystem fs = FileSystem.get(objConfiguration);
		fs.copyToLocalFile(new Path(sHadoopInputpath), new Path(
				sLocalOutputPath));
	}

	public void processOutPutFiles(Configuration objConfiguration,
			String sHadoopInputpath, String sLocalOutputPath, String sCoulmn1,
			String sCoulmn2) throws IOException {
		File objFile = null;
		File[] aFileArray = null;

		copyFromHadoopToLocal(objConfiguration, sHadoopInputpath,
				sLocalOutputPath);
		try {
			objFile = new File(sLocalOutputPath + "/data.tsv");
			aFileArray = new File(sLocalOutputPath).listFiles();
			mergeFiles(aFileArray, objFile, sCoulmn1, sCoulmn2);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void processOutPutFilesForRatings(String sLocalOutputPath,
			String sColumn1, String sColumn2) throws IOException {
		File objFile = null;
		BufferedWriter out = null;
		FileWriter fstream = null;
		try {
			objFile = new File(sLocalOutputPath + "/data.tsv");
			fstream = new FileWriter(objFile, true);
			out = new BufferedWriter(fstream);
			String sHeader = sColumn1 + "\t" + sColumn2;
			out.write(sHeader);
			out.newLine();
			mergeFilesRecursively(out, sLocalOutputPath);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			out.close();
		}
	}

	public static void mergeFiles(File[] files, File mergedFile,
			String sColumn1, String sColumn2) {

		FileWriter fstream = null;
		BufferedWriter out = null;
		BufferedReader in = null;
		String aLine = null;
		try {
			fstream = new FileWriter(mergedFile, true);
			out = new BufferedWriter(fstream);
			String sHeader = sColumn1 + "\t" + sColumn2;
			out.write(sHeader);
			out.newLine();
			for (File f : files) {
				System.out.println("merging: " + f.getName());
				FileInputStream fis;
				if (f.getName().startsWith("part-r-00")) {
					try {
						fis = new FileInputStream(f);
						in = new BufferedReader(new InputStreamReader(fis));
						while ((aLine = in.readLine()) != null) {
							out.write(aLine);
							out.newLine();
						}
						in.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		} finally {
			try {
				out.close();
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void mergeFilesRecursively(BufferedWriter out, String directoryName) {
		BufferedReader in = null;
		String aLine = null;
		File directory = new File(directoryName);
		// get all the files from a directory
		File[] fList = directory.listFiles();
		for (File file : fList) {
			try {
				if (file.isFile()) {
					FileInputStream fis;
					if (file.getName().startsWith("part-r-00")) {
						try {
							fis = new FileInputStream(file);
							in = new BufferedReader(new InputStreamReader(fis));
							while ((aLine = in.readLine()) != null) {
								out.write(aLine);
								out.newLine();
							}
						} catch (Exception e) {
							e.printStackTrace();
						} finally {
							in.close();
						}
					}
				} else if (file.isDirectory()) {
					mergeFilesRecursively(out, file.getCanonicalPath());
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
