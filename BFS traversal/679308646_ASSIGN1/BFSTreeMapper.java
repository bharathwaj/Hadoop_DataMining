package edu.uic.ids561.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BFSTreeMapper extends Mapper<Object, Text, Text, Text> {

	public void map(Object key, Text value, Context context, Node inN)
			throws IOException, InterruptedException {

		/* If the color is grey then the following properties are updated */
		if (inN.getColor() == Node.Color.GRAY) {
			for (String neighbor : inN.getEdges()) {

				Node adctNode = new Node();
				adctNode.setNodeid(neighbor);
				adctNode.setDistanceBw(inN.getDistanceBw() + 1);
				adctNode.setColor(Node.Color.GRAY);
				adctNode.setParentNode(inN.getNodeid());
				context.write(new Text(adctNode.getNodeid()),
						adctNode.getNodeInfo());
			}
			/* Decrementing the counter if the node is black */
			inN.setColor(Node.Color.BLACK);
			synchronized (Counter.obj) {
				Counter.Conter--;

			}

		}

		context.write(new Text(inN.getNodeid()), inN.getNodeInfo());

	}
}
