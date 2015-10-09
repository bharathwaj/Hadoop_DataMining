package edu.uic.ids561.EPL.Hadoop.other;

public class FilterWords {
	public static String removeStopWords(String text) {

		String remStopWords = "";

		// remove special characters
		text = text
				.replaceAll(
						"\\'|\"|[()]|\\/|\\_|\\[|\\]|\\$|\\*|\\=|\\`|\\>|\\<|\\?|\\&|\\+|\\|\\%|\\^|\\+",
						"");

		// remove punctuations
		text = text.replaceAll("\\.|\\,|\\?|\\!|\\:|\\;|\t|\\-", " ");

		// replace all spaces
		text = text.replaceAll("\\s+|\t|\n", " ");
		text = text.toLowerCase();

		//

		return text;
	}
}