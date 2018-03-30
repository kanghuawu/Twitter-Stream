package com.khwu.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;

public class PrintTweets {
    public static void main(String[] args) throws InterruptedException {
        Utilities.setUptTwitter();

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("PrintTweets");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1));

        Utilities.setUpLogging();

        JavaReceiverInputDStream<Status> tweets = TwitterUtils.createStream(ssc);

        JavaDStream<String> status = tweets.map(Status::getText);

        status.print();

        ssc.start();
        ssc.awaitTermination();
    }
}
