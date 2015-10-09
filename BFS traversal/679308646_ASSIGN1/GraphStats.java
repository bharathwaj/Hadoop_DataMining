package edu.uic.ids561.hadoop;

import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class GraphStats {

	public static void generatStats(String[] args) throws IOException {

		int NoNodes = 0, NoEdges = 0;
		/* Getting the input files */
		FileInputStream fs = new FileInputStream(args[0]);
		BufferedReader reader = new BufferedReader(new InputStreamReader(fs));
		// The path the file has to store.
		Path path = Paths.get("../TREE_DFS_ASII/output.txt");
		BufferedWriter writerBuffer = Files.newBufferedWriter(path,
				StandardCharsets.UTF_8);

		String currLine = null;
		while (true) {
			currLine = reader.readLine();
			if (currLine == null)
				break;
			NoNodes++;
			String[] lineArr = currLine.split("\t");
			String[] adjList = lineArr[1].split("\\|");
			writerBuffer.write(lineArr[0] + "\t"
					+ adjList[0].substring(0, adjList[0].length() - 1));
			writerBuffer.newLine();
			int length = adjList[0].split(",").length;
			NoEdges = NoEdges + length;
			length = 0;
		}
		/* Writting the contents of the file to */
		writerBuffer
				.write("\n BFS graph stats:\nTotal Number of Graph nodes in file: "
						+ NoNodes
						+ "\ntotal number of Graph edges in file: "
						+ NoEdges / 2);
		reader.close();

		writerBuffer.close();

	}

}
