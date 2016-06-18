package Streaming;

import java.util.Set;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public class SportScoreFunction implements PairFunction<Tuple2<Long, String>, Tuple2<Long, String>, Float> {

	public Tuple2<Tuple2<Long, String>, Float> call(Tuple2<Long, String> tweet) throws Exception {
		Set<String> sportWords = SportWords.getWords();
		String[] data = tweet._2().split(";");
		String tweetText = data[3];
		String text = tweetText.replaceAll("[^a-zA-Z\\s]", "").trim().toLowerCase();
		String[] words = text.split(" ");
		int totalWords = words.length;
		int numSportWords = 0;
		for (String word : words)
		{
		    if (sportWords.contains(word))
		    	numSportWords++;
		}
		return new Tuple2<Tuple2<Long, String>, Float>(
		    new Tuple2<Long, String>(tweet._1(), tweet._2()),
		    (float) numSportWords/totalWords
		);
	}

}