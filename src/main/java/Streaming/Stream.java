package Streaming;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import static java.lang.Math.toIntExact;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.HTTP;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import com.google.common.io.Files;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;
import scala.collection.generic.BitOperations.Int;
import twitter4j.Status;
import twitter4j.User;

public class Stream {
	
	// Possible values for Sentiment
	public static class Sentiment {
		public static String Positive = "Positive";
		public static String Negative = "Negative";
		public static String Neutral = "Neutral";
	}
	
	// Possible values for Category
	public static class Category {
		public static String General = "General";
		public static String Sports = "Sports";
		public static String Politics = "Politics";
		public static String Tech = "Tech";
	}
	
	public static void WriteInHbase(String sentiment, String score, String value) throws IOException {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.property.clientPort", "2182");
		Connection connection = ConnectionFactory.createConnection(config);
		Table table = connection.getTable(TableName.valueOf("analysis"));
		
		//Current date
		DateFormat dateFormat = new SimpleDateFormat("ddMMyyyy");
		Date date = new Date();
		
		//KEY
		String key = sentiment+dateFormat.format(date);
		Put sentimentAnalysisToInsert = new Put(Bytes.toBytes(key));
		
		//FAMILY - QUALIFIER - VALUE
		sentimentAnalysisToInsert.addColumn(Bytes.toBytes("sentiment"), Bytes.toBytes(score), Bytes.toBytes(value));
		table.put(sentimentAnalysisToInsert);
			
		table.close();
		connection.close();
	}
	
	public static void AddSentimentScore(String sentiment, float score) {	
		if(sentiment == Sentiment.Positive)
	    {
			PushDataToWebApplication("{\"positiveScore\": \""+score+"\", \"negativeScore\": \"0\"}");
		}
		else if(sentiment == Sentiment.Negative)
		{
			PushDataToWebApplication("{\"positiveScore\": \"0\", \"negativeScore\": \""+score+"\"}");
		}
	}
	
	public static void PushDataToWebApplication(String json) {
		//PUSH INFO TO WEB CLIENT: POSITIVE SCORE: X, NEGATIVE SCORE: Y.
		String webserver = "http://localhost:3000/sentiment-analysis";
		HttpClient client = new DefaultHttpClient();
		HttpPost post = new HttpPost(webserver);
		String content = json;
		try
		{
			StringEntity entity = new StringEntity(content, HTTP.UTF_8);
		    entity.setContentType("application/json");
		    post.setEntity(entity);
		    post.addHeader("Content-Type", "application/json");
		    HttpResponse response = client.execute(post);
		    org.apache.http.util.EntityUtils.consume(response.getEntity());
		}
		catch (Exception ex)
		{
			Logger LOG = Logger.getLogger("LOGGER");
            LOG.error("exception thrown while attempting to post", ex);
            LOG.trace(null, ex);
		}
	}

	@SuppressWarnings({ "deprecation", "serial" })
	public static void sentimentAnalysis(JavaDStream<Status> statuses) {
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
						String userName = tweet.getUser().getName();
						String location = tweet.getUser().getLocation();
						//String allUserInfo = tweet.getUser().toString();
						String numFollowers = String.valueOf(tweet.getUser().getFollowersCount());
						//String text = tweet.getText().replaceAll("[^a-zA-Z\\s]", "").trim().toLowerCase();
						String text = tweet.getText().trim().toLowerCase();
						String newText = "";
						for (String word : text.split(" ")) {
							if(!stopWords.contains(word))
								newText += word + " ";
						}
						return new Tuple2<Long, String>(tweet.getId(), userName + ";" + location + ";" + numFollowers+  ";" + 
														newText.trim().replaceAll(" +", " "));
					}
				});
		
		//mappedTweets.print();
		
		JavaPairDStream<Tuple2<Long, String>, Float> positiveTweets =
				mappedTweets.mapToPair(new PositiveScoreFunction());
		
		//positiveTweets.print();
		
		JavaPairDStream<Tuple2<Long, String>, Float> negativeTweets =
				mappedTweets.mapToPair(new NegativeScoreFunction());
		
		//negativeTweets.print();
		
//		JavaPairDStream<Tuple2<Long, String>, Float> sportTweets =
//				mappedTweets.mapToPair(new SportScoreFunction());
//		
//		sportTweets.print();
//		
//		JavaPairDStream<Tuple2<Long, String>, Float> politicsTweets =
//				mappedTweets.mapToPair(new PoliticsScoreFunction());
//		
//		politicsTweets.print();
//		
//		JavaPairDStream<Tuple2<Long, String>, Float> techTweets =
//				mappedTweets.mapToPair(new TechScoreFunction());
//		
//		techTweets.print();

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
		
		finalResult.foreachRDD(new Function<JavaRDD<Tuple5<Long,String,Float,Float,String>>, Void>() {
			
			public Void call(JavaRDD<Tuple5<Long, String, Float, Float, String>> tweets) throws Exception {
				for (Tuple5<Long, String, Float, Float, String> tweet : tweets.take(toIntExact(tweets.count()))) {
					String[] data = tweet._2().split(";");
					String userName = data[0];
					String location = data[1];
					String numFollowers = data[2];
					String tweetText = data[3];
					
					long idTweet = tweet._1();
					float posScore = tweet._3();
					float negScore = tweet._4();
					String sentiment = tweet._5();
					
					if (posScore > 0.2 || negScore > 0.2 || Integer.parseInt(numFollowers) > 10000)
					{
						String newLine = System.lineSeparator();
						
//						System.out.println(	"Tweet ID: " + idTweet + newLine +
//											"User name: " + userName + newLine +
//											"Location: " + location + newLine +
//											"Nº followers: " + numFollowers + newLine + 
//											"Tweet: " + tweetText + newLine +
//											"Positive Score: " + posScore + newLine +
//											"Negative Score: " + negScore + newLine +
//											"Sentiment: " + sentiment + newLine);
//						PushDataToWebApplication("{\"positiveScore\": \"0\", \"negativeScore\": \""+score+"\"}");
//						PushDataToWebApplication("{\"positiveScore\": \""+score+"\", \"negativeScore\": \"0\"}");
						
						String json = "{\"idTweet\": \""+idTweet+"\", "
								+ "\"user\": \""+userName+"\", "
								+ "\"location\": \""+location+"\", "
								+ "\"numFollowers\": \""+numFollowers+"\", "
								+ "\"tweet\": \""+tweetText+"\" , "
								+ "\"posScore\": \""+posScore+"\" , "
								+ "\"negScore\": \""+negScore+"\" , "
								+ "\"sentiment\": \""+sentiment+"\"}";
						
						System.out.println(json);
								
//						PushDataToWebApplication(json);
//						
//						if (sentiment == Sentiment.Positive) {
//							WriteInHbase(Sentiment.Positive, String.valueOf(posScore), json);
//						}
//						else if (sentiment == Sentiment.Negative) {
//							WriteInHbase(Sentiment.Negative, String.valueOf(negScore), json);
//						}
					}
				}
				return null;
			}
		});
		
		//finalResult.print();
	}
}
