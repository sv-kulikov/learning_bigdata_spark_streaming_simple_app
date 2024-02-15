package biz.svyatoslav.learning.bigdata.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class StreamingSimpleApp {
    public static void main(String[] args) {

        // If you get errors like "Exception in thread "main" java.lang.IllegalAccessError:
        //  class org.apache.spark.storage.StorageUtils$ (in unnamed module ...)
        //  cannot access class sun.nio.ch.DirectBuffer (in module java.base)
        //  because module java.base does not export sun.nio.ch to unnamed module ..."
        // Follow this advice: https://stackoverflow.com/questions/73465937/apache-spark-3-3-0-breaks-on-java-17-with-cannot-access-class-sun-nio-ch-direct
        // In short: add this JVM Option in IDEA IDE: "--add-exports java.base/sun.nio.ch=ALL-UNNAMED"


        // 1) Open terminal window (Ctrl+Alt+T).
        // 2) Execute "nc -lk 9999" in terminal window.
        // 3) Run this application in IntelliJ IDEA.
        // 4) Type something like "ABC XYZ ABC DEF" in terminal window and press Enter.
        //
        // In IntelliJ IDEA you will see something like this:
        //
        // -------------------------------------------
        // Time: 1706449138000 ms
        // -------------------------------------------
        // (ABC,2)
        // (DEF,1)
        // (XYZ,1)

        // Create a local StreamingContext with two working thread and batch interval of 1 second
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        // Create a DStream that will connect to hostname:port, like localhost:9999
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

        // Split each line into words
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        // Count each word in each batch
        JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);

        // Print the first ten elements of each RDD generated in this DStream to the console
        wordCounts.print();

        // Start the computation
        jssc.start();
        try {
            // Wait for the computation to terminate
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}