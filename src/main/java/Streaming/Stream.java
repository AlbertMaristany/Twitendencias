package Streaming;

import java.util.List;
import java.util.Set;

import org.apache.log4j.BasicConfigurator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;
import twitter4j.Status;

public class Stream {

	public static void sentimentAnalysis(JavaDStream<Status> statuses, final SQLContext sqlctx) {
		JavaDStream<Status> spanishTweets = statuses
				.filter(new Function<Status, Boolean>() {
					public Boolean call(Status tweet) throws Exception {
						if(tweet == null)
							return false;
						return LanguageDetector.isSpanish(tweet.getText()) || LanguageDetector.isCatalan(tweet.getText());
					}
				});
		
		//englishTweets.print();
		
		JavaPairDStream<Long, String> mappedTweets = spanishTweets
				.mapToPair(new PairFunction<Status, Long, String>() {
					public Tuple2<Long, String> call(Status tweet) throws Exception {
						List<String> stopWords = StopWords.getWords();
						//String text = tweet.getText().replaceAll("[^a-zA-Z\\s]", "").trim().toLowerCase();
						String text = tweet.getText().trim().toLowerCase();
						String newText = "";
						for (String word : text.split(" ")) {
							if(!stopWords.contains(word))
								newText += word + " ";
						}
						return new Tuple2<Long, String>(tweet.getId(), newText.trim().replaceAll(" +", " "));
					}
				});
		
		//mappedTweets.print();
		
		JavaPairDStream<Tuple2<Long, String>, Float> positiveTweets =
				mappedTweets.mapToPair(new PositiveScoreFunction());
		
		//positiveTweets.print();
		
		JavaPairDStream<Tuple2<Long, String>, Float> negativeTweets =
				mappedTweets.mapToPair(new NegativeScoreFunction());
		
		//negativeTweets.print();
		
		JavaPairDStream<Tuple2<Long, String>, Float> sportTweets =
				mappedTweets.mapToPair(new SportScoreFunction());
		
		sportTweets.print();
		
		JavaPairDStream<Tuple2<Long, String>, Float> politicsTweets =
				mappedTweets.mapToPair(new PoliticsScoreFunction());
		
		politicsTweets.print();
		
		JavaPairDStream<Tuple2<Long, String>, Float> techTweets =
				mappedTweets.mapToPair(new TechScoreFunction());
		
		techTweets.print();

		JavaPairDStream<Tuple2<Long, String>, Tuple2<Float, Float>> joinedTweets =
			positiveTweets.join(negativeTweets);
		
		//joinedTweets.print();

		JavaDStream<Tuple4<Long, String, Float, Float>> scoredTweets =
					joinedTweets.map(new Function<Tuple2<Tuple2<Long, String>, Tuple2<Float, Float>>, Tuple4<Long, String, Float, Float>>() {
			    public Tuple4<Long, String, Float, Float> call(
			        Tuple2<Tuple2<Long, String>, Tuple2<Float, Float>> tweet) {
			        return new Tuple4<Long, String, Float, Float>(
			            tweet._1()._1(), tweet._1()._2(), tweet._2()._1(), tweet._2()._2());
			    }
			});

		JavaDStream<Tuple5<Long, String, Float, Float, String>> finalResult =
			    scoredTweets.map(new ScoreTweetsFunction());
		
		//finalResult.print();
	}
}
