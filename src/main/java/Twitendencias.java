import java.io.File;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import com.google.common.io.Files;

import Streaming.Stream;
import twitter4j.Status;


public class Twitendencias {

	static String TWITTER_CONFIG_PATH = "src/main/resources/twitter_configuration.txt";
	static String HADOOP_COMMON_PATH = "/home/osboxes/hadoop-2.5.1";
	
	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", HADOOP_COMMON_PATH);
		SparkConf conf = new SparkConf().setAppName("Twitendencias").setMaster("local[*]");
		JavaSparkContext ctx = new JavaSparkContext(conf);
		JavaStreamingContext jsc = new JavaStreamingContext(ctx, new Duration(5000));
		LogManager.getRootLogger().setLevel(Level.ERROR);
		
		jsc.checkpoint(Files.createTempDir().getAbsolutePath());
				
		Utils.setupTwitter(TWITTER_CONFIG_PATH);
		
		JavaDStream<Status> tweets = TwitterUtils.createStream(jsc);
		
		Stream.sentimentAnalysis(tweets);	
		
		jsc.start();
		jsc.awaitTermination();
	}

}
