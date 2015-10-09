package edu.uic.ids561.mahout.collfilter.item;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.recommender.ItemBasedRecommender;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

public class RecommenderTest {

	public static void main(String args[]) {
		try {
			FileDataModel itdataModel = new FileDataModel(new File("input.csv"));
			ItemSimilarity itemSimilarity = new PearsonCorrelationSimilarity(
					itdataModel);
			ItemBasedRecommender recommender = new GenericItemBasedRecommender(
					itdataModel, itemSimilarity);
			List<RecommendedItem> recommendations = recommender.recommend(2, 3);
			for (RecommendedItem recommendedItem : recommendations) {
				System.out.println(recommendedItem.getItemID());
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (TasteException e) {
			e.printStackTrace();
		}

	}
}
