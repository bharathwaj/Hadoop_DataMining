package edu.uic.ids561.decision;

import java.util.ArrayList;

public class DTreeConst {
	public DTreeNode buildTree(ArrayList<DTreeRecord> records, DTreeNode root,
			DTreeLearningSet learningSet) {
		int bestAttribute = -1;
		double bestGain = 0;
		root.setEntropy(DTreeEntropy.calculateEntropy(root.getData()));

		if (root.getEntropy() == 0) {
			return root;
		}

		for (int i = 0; i < DecisionTreeMain.NUM_ATTRS - 2; i++) {
			if (!DecisionTreeMain.isAttributeUsed(i)) {
				double entropy = 0;
				ArrayList<Double> entropies = new ArrayList<Double>();
				ArrayList<Integer> setSizes = new ArrayList<Integer>();

				for (int j = 0; j < DecisionTreeMain.NUM_ATTRS - 2; j++) {
					ArrayList<DTreeRecord> subset = subset(root, i, j);
					setSizes.add(subset.size());

					if (subset.size() != 0) {
						entropy = DTreeEntropy.calculateEntropy(subset);
						entropies.add(entropy);
					}
				}

				double gain = DTreeEntropy.calculateGain(root.getEntropy(),
						entropies, setSizes, root.getData().size());

				if (gain > bestGain) {
					bestAttribute = i;
					bestGain = gain;
				}
			}
		}
		if (bestAttribute != -1) {
			int setSize = DecisionTreeMain.setSize(DecisionTreeMain.attrMap
					.get(bestAttribute));
			root.setTestAttribute(new DiscreteAttribute(
					DecisionTreeMain.attrMap.get(bestAttribute), 0));
			root.children = new DTreeNode[setSize];
			root.setUsed(true);
			DecisionTreeMain.usedAttributes.add(bestAttribute);

			for (int j = 0; j < setSize; j++) {
				root.children[j] = new DTreeNode();
				root.children[j].setParent(root);
				System.out.println("Damm" + root.children[j]);
				root.children[j].setData(subset(root, bestAttribute, j));
				root.children[j].getTestAttribute().setName(
						DecisionTreeMain.getLeafNames(bestAttribute, j));
				root.children[j].getTestAttribute().setValue(j);
			}

			for (int j = 0; j < setSize; j++) {
				buildTree(root.children[j].getData(), root.children[j],
						learningSet);
			}

			root.setData(null);
		} else {
			return root;
		}

		return root;
	}

	public ArrayList<DTreeRecord> subset(DTreeNode root, int attr, int value) {
		ArrayList<DTreeRecord> subset = new ArrayList<DTreeRecord>();

		for (int i = 0; i < root.getData().size(); i++) {
			DTreeRecord record = root.getData().get(i);

			if (record.getAttributes().get(attr).getValue() == value) {
				subset.add(record);
			}
		}
		return subset;
	}

	public double calculateSurrogates(ArrayList<DTreeRecord> records) {
		return 0;

	}

	public void pruneTree() {

	}
}
