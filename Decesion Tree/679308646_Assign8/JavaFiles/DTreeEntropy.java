package edu.uic.ids561.decision;

import java.util.ArrayList;

public class DTreeEntropy {
	public static double calculateEntropy(ArrayList<DTreeRecord> data) {
		double entropy = 0;

		if (data.size() == 0) {
			// nothing to do
			return 0;
		}

		for (int i = 0; i < DecisionTreeMain.setSize("PlayTennis"); i++) {
			int count = 0;
			for (int j = 0; j < data.size(); j++) {
				DTreeRecord record = data.get(j);

				if (record.getAttributes().get(4).getValue() == i) {
					count++;
				}
			}

			double probability = count / (double) data.size();
			if (count > 0) {
				entropy += -probability * (Math.log(probability) / Math.log(2));
			}
		}
		 System.out.println("The calculated Entropy" + entropy);

		return entropy;
	}

	public static double calculateGain(double rootEntropy,
			ArrayList<Double> subEntropies, ArrayList<Integer> setSizes,
			int data) {
		double gain = rootEntropy;

		for (int i = 0; i < subEntropies.size(); i++) {
			gain += -((setSizes.get(i) / (double) data) * subEntropies.get(i));
		}
		 System.out.println("The caclulated Gain" + gain);
		return gain;
	}
}
