package Streaming;

import java.util.Locale.Category;

import org.apache.spark.api.java.function.Function;

import scala.Tuple6;
import scala.Tuple7;

@SuppressWarnings("serial")
public class ScoreTweetsFunction
		implements Function<Tuple7<Long, String, Float, Float, Float, Float, Float>, Tuple6<Long, String, Float, Float, String, String>> {
	public Tuple6<Long, String, Float, Float, String, String> call(Tuple7<Long, String, Float, Float, Float, Float, Float> tweet) throws Exception {
		/* Params from Tuple7
		 * 1) Long: tweet id
		 * 2) String: text
		 * 3) Float: positive score
		 * 4) Float: negative score
		 * 5) Float: sports
		 * 6) Float: politics
		 * 7) Float: tech
		 */
		String score;
		if (tweet._3() > tweet._4()) score = Stream.Sentiment.Positive;
		else if (tweet._3() < tweet._4()) score = Stream.Sentiment.Negative;
		else score = Stream.Sentiment.Neutral;
		
		String category;
		if (tweet._5() > tweet._6() && tweet._5() > tweet._7()) category = Stream.Category.Sports;
		else if (tweet._6() > tweet._5() && tweet._6() > tweet._7()) category = Stream.Category.Politics;
		else if (tweet._7() > tweet._5() && tweet._7() > tweet._6()) category = Stream.Category.Tech;
		else category = Stream.Category.General;
		
		return new Tuple6<Long, String, Float, Float, String, String>(
		    tweet._1(), tweet._2(), tweet._3(), tweet._4(), score, category);
	}

}
