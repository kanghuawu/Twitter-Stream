package com.khwu.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.apache.spark.util.LongAccumulator;
import scala.Option;
import twitter4j.Status;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class SaveTweets {

    /*
    ref: https://www.linkedin.com/pulse/how-shutdown-spark-streaming-job-gracefully-lan-jiang/
    source code: https://github.com/lanjiang/streamingstopgraceful
     */

    private static String shutdownMarker = "./stop/shutdownmarker.txt";
    private static boolean shutdownFlag= false;


    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("PrintTweets");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(2));

        Utilities.setUpLogging();
        Utilities.setUptTwitter();

        JavaReceiverInputDStream<Status> tweets = TwitterUtils.createStream(ssc);

        final LongAccumulator totalTweets = new LongAccumulator();
        totalTweets.register(ssc.sparkContext().sc(), Option.apply("totalTweets"), false);

        tweets.foreachRDD((statusJavaRDD, time) -> {
            if (statusJavaRDD.count() > 0) {
                JavaRDD<Status> repartitionRDD = statusJavaRDD.repartition(1).cache();
                repartitionRDD.saveAsTextFile("./Tweets_" + time.milliseconds());
                totalTweets.add(repartitionRDD.count());
            }
        });

        ssc.checkpoint("/tmp/checkpoint/");
        ssc.start();

        int checkIntervalMilis = 10000;
        boolean isStopped = false;

        while (!isStopped) {
            System.out.println("calling awaitTerminationOrTimeout");
            isStopped = ssc.awaitTerminationOrTimeout(checkIntervalMilis);
            if (isStopped) System.out.println("confirmed! The streaming context is stopped. Exiting application...");
            else System.out.println("Streaming App is still running. Timeout...");
            checkShutdownMarker(totalTweets);

            if (!isStopped && shutdownFlag) {
                System.out.println("stopping ssc right now");
                ssc.stop(true, true);
                System.out.println("ssc is stopped");
            }
        }
    }

    private static void checkShutdownMarker(LongAccumulator totalTweets) {
        /*
        if (!shutdownFlag && totalTweets.value() > 200) {
            shutdownFlag = true;
        }
        */
        if (!shutdownFlag) {
            Path path = Paths.get(shutdownMarker);
            if (Files.exists(path)) {
                shutdownFlag = true;
            }
        }
    }
}
