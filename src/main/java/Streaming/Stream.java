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
import scala.Tuple6;
import scala.Tuple7;
import scala.collection.generic.BitOperations.Int;
import twitter4j.Status;
import twitter4j.User;

public class Stream {
	
	// Possible values for Tables
	public static class HBaseTableName {
		public static String Mood = "mood";
		public static String MinMood = "minmood";
		public static String Summaries = "summaries";
		public static String Categories = "categories";
	}
		
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
	
	public static void WriteInHbase(String score, String tableName, String value) throws IOException {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.property.clientPort", "2182");
		Connection connection = ConnectionFactory.createConnection(config);
		Table table = connection.getTable(TableName.valueOf(tableName));
		
		//Current date
		DateFormat dateFormat = new SimpleDateFormat("ddMMyyyy");
		Date date = new Date();
		
		//KEY
		String key = dateFormat.format(date);
		Put sentimentAnalysisToInsert = new Put(Bytes.toBytes(key));
		
		//FAMILY - QUALIFIER - VALUE
		sentimentAnalysisToInsert.addColumn(Bytes.toBytes("data"), Bytes.toBytes(score), Bytes.toBytes(value));
		table.put(sentimentAnalysisToInsert);
			
		table.close();
		connection.close();
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
		
		//spanishTweets.print();
		
		// Get relevant information from tweets
		JavaPairDStream<Long, String> mappedTweets = spanishTweets
				.mapToPair(new PairFunction<Status, Long, String>() {
					public Tuple2<Long, String> call(Status tweet) throws Exception {
						// Finally not deleting useless words for tweet understanding purposes
						//List<String> stopWords = StopWords.getWords();
						String userName = tweet.getUser().getName();
						String location = tweet.getUser().getLocation();
						//String allUserInfo = tweet.getUser().toString();
						String numFollowers = String.valueOf(tweet.getUser().getFollowersCount());
						//String text = tweet.getText().replaceAll("[^a-zA-Z\\s]", "").trim().toLowerCase();
						String text = tweet.getText().trim();
						/*String newText = "";
						for (String word : text.split(" ")) {
							if(!stopWords.contains(word))
								newText += word + " ";
						}*/
						return new Tuple2<Long, String>(tweet.getId(), userName + ";" + location + ";" + numFollowers+  ";" + 
														text.trim()/*.replaceAll(" +", " ")*/);
					}
				});
		
		//mappedTweets.print();
		
		// Positive scoring
		JavaPairDStream<Tuple2<Long, String>, Float> positiveTweets =
				mappedTweets.mapToPair(new PositiveScoreFunction());
		
		//positiveTweets.print();
		
		// Negative scoring
		JavaPairDStream<Tuple2<Long, String>, Float> negativeTweets =
				mappedTweets.mapToPair(new NegativeScoreFunction());
		
		//negativeTweets.print();
		
		// Sports scoring
		JavaPairDStream<Tuple2<Long, String>, Float> sportTweets =
				mappedTweets.mapToPair(new SportScoreFunction());
		
		//sportTweets.print();
		
		// Politics scoring
		JavaPairDStream<Tuple2<Long, String>, Float> politicsTweets =
				mappedTweets.mapToPair(new PoliticsScoreFunction());
		
		//politicsTweets.print();
		
		// Tech scoring
		JavaPairDStream<Tuple2<Long, String>, Float> techTweets =
				mappedTweets.mapToPair(new TechScoreFunction());
		
		//techTweets.print();

		// JOINS
		JavaPairDStream<Tuple2<Long, String>, Tuple2<Float, Float>> joinedTweets =
			positiveTweets.join(negativeTweets);
		
		JavaPairDStream<Tuple2<Long, String>, Tuple2<Tuple2<Float, Float>, Float>> joinWithSports =
				joinedTweets.join(sportTweets);
		
		JavaPairDStream<Tuple2<Long, String>, Tuple2<Tuple2<Tuple2<Float,Float>,Float>,Float>> joinWithPolitics =
				joinWithSports.join(politicsTweets);
		
		JavaPairDStream<Tuple2<Long, String>, Tuple2<Tuple2<Tuple2<Tuple2<Float,Float>,Float>,Float>,Float>> joinWithTech =
				joinWithPolitics.join(techTweets);
		
		//joinWithTech.print();
		
		// Map and reduce information
		JavaDStream<Tuple7<Long, String, Float, Float, Float, Float, Float>> finalJoin =
				joinWithTech.map(new Function<Tuple2<Tuple2<Long, String>, Tuple2<Tuple2<Tuple2<Tuple2<Float,Float>,Float>,Float>,Float>>,
						Tuple7<Long, String, Float, Float, Float, Float, Float>>() {
					public Tuple7<Long, String, Float, Float, Float, Float, Float> call(
							Tuple2<Tuple2<Long, String>, Tuple2<Tuple2<Tuple2<Tuple2<Float,Float>,Float>,Float>,Float>> tweet) {
						return new Tuple7<Long, String, Float, Float, Float, Float, Float>(
								tweet._1()._1(), // id
								tweet._1()._2(), // text
								tweet._2()._1()._1()._1()._1(), // positive
								tweet._2()._1()._1()._1()._2(), // negative
								tweet._2()._1()._1()._2, // sports
								tweet._2()._1()._2, // politics
								tweet._2()._2); // tech
					}
				});
		
		//finalJoin.print();

		// Get final score and category
		JavaDStream<Tuple6<Long, String, Float, Float, String, String>> finalResult =
			    finalJoin.map(new ScoreTweetsFunction());
		
		// Process data and send to web app and hbase
		finalResult.foreachRDD(new Function<JavaRDD<Tuple6<Long,String,Float,Float,String,String>>, Void>() {
			
			public Void call(JavaRDD<Tuple6<Long, String, Float, Float, String, String>> tweets) throws Exception {
				for (Tuple6<Long, String, Float, Float, String, String> tweet : tweets.take((int)tweets.count())) {
					String[] data = tweet._2().split(";");
					String userName = data[0];
					String location = data[1] == null ? " " : data[1];
					String numFollowers = data[2];
					String tweetText = data[3];
					
					long idTweet = tweet._1();
					float posScore = tweet._3();
					float negScore = tweet._4();
					String sentiment = tweet._5();
					String category = tweet._6();
					int totalFollowers = Integer.parseInt(numFollowers);
					
					String jsonMinified = "{\"idTweet\": \""+idTweet+"\", "
							+ "\"user\": \""+userName+"\", "
							+ "\"posScore\": \""+posScore+"\","
							+ "\"negScore\": \""+negScore+"\","
							+ "\"category\": \""+category+"\"}";
					
					String json = "{\"idTweet\": \""+idTweet+"\", "
							+ "\"user\": \""+userName+"\", "
							+ "\"location\": \""+location+"\", "
							+ "\"numFollowers\": \""+numFollowers+"\", "
							+ "\"tweet\": \""+tweetText+"\" , "
							+ "\"posScore\": \""+posScore+"\" , "
							+ "\"negScore\": \""+negScore+"\" , "
							+ "\"sentiment\": \""+sentiment+"\" , "
							+ "\"category\": \""+category+"\"}";
					
					//System.out.println(json);
					
					if(posScore > negScore)
					{					
						WriteInHbase(String.valueOf(posScore), HBaseTableName.MinMood, jsonMinified);
						
						if(posScore > 0.2 || totalFollowers > 10000)
					    {
							System.out.println(json);
							
							WriteInHbase(String.valueOf(posScore), HBaseTableName.Mood, json);
						}
						
						PushDataToWebApplication(json);				
					}
					else if (posScore < negScore)
					{
						WriteInHbase(String.valueOf(negScore), HBaseTableName.MinMood, jsonMinified);
						
						if(negScore > 0.2 || totalFollowers > 10000)
					    {	
							WriteInHbase(String.valueOf(negScore), HBaseTableName.Mood, json);
						}
						
						PushDataToWebApplication(json);
					}
					else if (category != Category.General && (posScore > 0 || negScore > 0))
					{
						PushDataToWebApplication(json);
					}
				}
				return null;
			}
		});
	}
}