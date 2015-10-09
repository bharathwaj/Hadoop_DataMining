package edu.uic.ids561.sampling;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Random;

public class ReservoirSampling {

	public static void main(String[] args) {
		// Select the value for K
		int k = 45;
		// Creating array for the input
		int[] jResvoirvar = new int[10000];
		int i = 0;

		try {
			BufferedReader br = new BufferedReader(new FileReader("input.txt"));
			String line;
			while ((line = br.readLine()) != null) {
				// process the line.
				jResvoirvar[i] = Integer.parseInt(line);
				i++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		int n = jResvoirvar.length;
		resvoirsampleItems(jResvoirvar, n, k);
	}

	public static void resvoirsampleItems(int Resvoirvar[], int n, int k) {
		int i;
		Random random = new Random(System.currentTimeMillis());
		int[] reservoirArray = new int[k];
		for (i = 0; i < k; i++) {
			reservoirArray[i] = Resvoirvar[i];
		}

		for (; i < n; i++) {
			int j = random.nextInt(i) % (i + 1);
			// System.out.println("the value of j" + j);
			if (j < k) {
				reservoirArray[j] = Resvoirvar[i];
			}
		}

		printresult(reservoirArray, k);
	}

	public static void printresult(int stream[], int n) {
		for (int i = 0; i < n; i++) {
			System.out.println(stream[i]);
		}
	}

}
