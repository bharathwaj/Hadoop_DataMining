package edu.uic.ids561.decision;

import java.util.ArrayList;

public class DecisionTreeMain {
	public static int NUM_ATTRS = 5;
	public static ArrayList<String> attrMap;
	public static ArrayList<Integer> usedAttributes = new ArrayList<Integer>();

	public static void main(String[] args) {
		attrMap = new ArrayList<String>();
		attrMap.add("Outlook");
		attrMap.add("Temperature");
		attrMap.add("Humidity");
		attrMap.add("Wind");
		attrMap.add("PlayTennis");

		DTreeConst t = new DTreeConst();
		ArrayList<DTreeRecord> records;
		DTreeLearningSet learningSet = new DTreeLearningSet();

		// read in all our data
		records = DTreeFileReader.buildRecords();
		DTreeNode root = new DTreeNode();

		for (DTreeRecord record : records) {
			root.getData().add(record);
		}
		System.out.println("The Root" + root.getData().size());
		t.buildTree(records, root, learningSet);
		traverseTree(records.get(12), root);
		return;
	}

	public static Double traverseTree(DTreeRecord r, DTreeNode root) {
		if (root.children == null)
			return root.getTestAttribute().getValue();
		double nodeValue = 0;
		for (int i = 0; i < r.getAttributes().size(); i++) {
			if (r.getAttributes().get(i).getName()
					.equalsIgnoreCase(root.getTestAttribute().getName())) {
				nodeValue = r.getAttributes().get(i).getValue();
				break;
			}
		}
		for (int i = 0; i < root.getChildren().length; i++) {
			if (nodeValue == root.children[i].getTestAttribute().getValue()) {
				traverseTree(r, root.children[i]);
			}
		}
		return root.getTestAttribute().getValue();
	}

	public static boolean isAttributeUsed(int attribute) {
		if (usedAttributes.contains(attribute)) {
			return true;
		} else {
			return false;
		}
	}

	public static int setSize(String set) {
		if (set.equalsIgnoreCase("Outlook")) {
			return 3;
		} else if (set.equalsIgnoreCase("Wind")) {
			return 2;
		} else if (set.equalsIgnoreCase("Temperature")) {
			return 3;
		} else if (set.equalsIgnoreCase("Humidity")) {
			return 2;
		} else if (set.equalsIgnoreCase("PlayTennis")) {
			return 2;
		}
		return 0;
	}

	public static String getLeafNames(int attributeNum, int valueNum) {
		if (attributeNum == 0) {
			if (valueNum == 0) {
				return "Sunny";
			} else if (valueNum == 1) {
				return "Overcast";
			} else if (valueNum == 2) {
				return "Rain";
			}
		} else if (attributeNum == 1) {
			if (valueNum == 0) {
				return "Hot";
			} else if (valueNum == 1) {
				return "Mild";
			} else if (valueNum == 2) {
				return "Cool";
			}
		} else if (attributeNum == 2) {
			if (valueNum == 0) {
				return "High";
			} else if (valueNum == 1) {
				return "Normal";
			}
		} else if (attributeNum == 3) {
			if (valueNum == 0) {
				return "Weak";
			} else if (valueNum == 1) {
				return "Strong";
			}
		}

		return null;
	}

}
