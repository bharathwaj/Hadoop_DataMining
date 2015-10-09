package edu.uic.ids561.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BFSTreeReducer extends Reducer<Text, Text, Text, Text> {

	public Node reduce(Text key, Iterable<Text> values, Context context,
			Node out) throws IOException, InterruptedException {

		out.setNodeid(key.toString());

		for (Text value : values) {

			Node inN = new Node(key.toString() + "\t" + value.toString());
			if (inN.getEdges().size() > 0) {
				out.setEdges(inN.getEdges());
			}
			if (inN.getDistanceBw() < out.getDistanceBw()) {
				out.setDistanceBw(inN.getDistanceBw());
				out.setParentNode(inN.getParentNode());
			}
			if (inN.getColor().ordinal() > out.getColor().ordinal()) {
				out.setColor(inN.getColor());
			}
		}
		/* Incrementing the counter if the the node color is grey */
		/* The use of synchronized method used */
		if (out.getColor() == Node.Color.GRAY) {
			synchronized (Counter.obj) {
				Counter.Conter++;
			}
		}
		context.write(key, new Text(out.getNodeInfo()));
		return out;

	}
}
