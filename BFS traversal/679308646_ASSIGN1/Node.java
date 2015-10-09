package edu.uic.ids561.hadoop;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;

public class Node {

	// Declaring variables that are required store the details for the Node

	private String nodeid;
	private int distanceBw;
	private List<String> edges = new ArrayList<String>();
	private Color color = Color.WHITE;
	private String parentNode;

	public Node() {

		edges = new ArrayList<String>();
		distanceBw = Integer.MAX_VALUE;
		color = Color.WHITE;
		setParentNode(null);
	}

	public Node(String nodeInfo) {
		/* reads the input line nand splits the data based on Tab seprations */
		String[] inputLine = nodeInfo.split("\t");
		String key = "", value = "";
		try {
			key = inputLine[0];
			/*System.out.println(key);*/
			value = inputLine[1];

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}

		String[] tokens = value.split("\\|");

		this.nodeid = key;

		for (String s : tokens[0].split(",")) {
			if (s.length() > 0) {
				edges.add(s);
			}
		}
		if (Counter.sourceNode == Long.parseLong(key.toString())
				&& (Counter.jobConter == 1)) {
//			System.out.println("Hello World");

			this.setColor(Node.Color.GRAY);
			this.setDistanceBw(0);
			this.setParentNode("Source");
		} else {

			if (tokens[1].equals("Integer.MAX_VALUE")) {
				this.distanceBw = Integer.MAX_VALUE;
			} else {
				this.distanceBw = Integer.parseInt(tokens[1]);
			}
			this.color = Color.valueOf(tokens[2]);
			this.setParentNode(tokens[3]);
		}
	}

	public Text getNodeInfo() {

		StringBuffer s = new StringBuffer();
		try {
			for (String v : edges) {
				s.append(v).append(",");
			}
		} catch (NullPointerException e) {
			e.printStackTrace();
			System.exit(1);
		}
		s.append("|");
		if (this.distanceBw < Integer.MAX_VALUE) {
			s.append(this.distanceBw).append("|");
		} else {
			s.append("Integer.MAX_VALUE").append("|");
		}
		s.append(color.toString()).append("|");
		s.append(getParentNode());
		return new Text(s.toString());

	}

	public String getNodeid() {
		return nodeid;
	}

	public void setNodeid(String nodeid) {
		this.nodeid = nodeid;
	}

	public int getDistanceBw() {
		return distanceBw;
	}

	public void setDistanceBw(int distanceBw) {
		this.distanceBw = distanceBw;
	}

	public String getParentNode() {
		return parentNode;
	}

	public void setParentNode(String parentNode) {
		this.parentNode = parentNode;
	}

	public List<String> getEdges() {
		return edges;
	}

	public void setEdges(List<String> edges) {
		this.edges = edges;
	}

	public Color getColor() {
		return color;
	}

	public void setColor(Color color) {
		this.color = color;
	}

	public static enum Color {
		WHITE, GRAY, BLACK
	};

}
