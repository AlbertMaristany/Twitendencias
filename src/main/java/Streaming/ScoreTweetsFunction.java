package Streaming;

import org.apache.spark.api.java.function.Function;

import scala.Tuple4;
import scala.Tuple5;

@SuppressWarnings("serial")
public class ScoreTweetsFunction
		implements Function<Tuple4<Long, String, Float, Float>, Tuple5<Long, String, Float, Float, String>> {

	public Tuple5<Long, String, Float, Float, String> call(Tuple4<Long, String, Float, Float> tweet) throws Exception {
		String score;
		if (tweet._3() > tweet._4()) score = "positive";
		else if (tweet._3() < tweet._4()) score = "negative";
		else score = "neutral";
		
		return new Tuple5<Long, String, Float, Float, String>(
		    tweet._1(), tweet._2(), tweet._3(), tweet._4(), score);
	}

}
