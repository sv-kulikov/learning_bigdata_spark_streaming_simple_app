package biz.svyatoslav.learning.bigdata.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Arrays;

// If you get errors like "Exception in thread "main" java.lang.IllegalAccessError:
//  class org.apache.spark.storage.StorageUtils$ (in unnamed module ...)
//  cannot access class sun.nio.ch.DirectBuffer (in module java.base)
//  because module java.base does not export sun.nio.ch to unnamed module ..."
// Follow this advice: https://stackoverflow.com/questions/73465937/apache-spark-3-3-0-breaks-on-java-17-with-cannot-access-class-sun-nio-ch-direct
// In short: add this JVM Option in IDEA IDE: "--add-exports java.base/sun.nio.ch=ALL-UNNAMED"

// 1) In Linux: open terminal window (Ctrl+Alt+T).
//    In Windows: open CMD console.
// 2) In Linux: execute "nc -lk 9999" in terminal window.
//    (For Linux use: sudo apt install nc -y)
//    In Windows: execute "ncat -l -p 9999 --keep-open"
//    (For Windows use https://nmap.org/download.html#windows)
// 3) Run this application in IntelliJ IDEA.
// 4) Type something like "ABC XYZ ABC DEF" in terminal window and press Enter.

public class StreamingSimpleApp {
    private static final String HOST = "localhost";
    private static final int PORT = 9999;

    public static void main(String[] args) {

        // Check if the socket server is available before starting Spark Streaming
        if (!isPortOpen(HOST, PORT, 2000)) {
            System.err.println("ERROR: Cannot connect to " + HOST + ":" + PORT +
                    ". Ensure Netcat/Ncat is running before starting this application.");
            return;
        } else {
            System.out.println("Ncat detected at " + HOST + ":" + PORT + ". Ready to receive data...");
        }

        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("NetworkWordCount")
                .set("spark.ui.showConsoleProgress", "false");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        // Define the input stream
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream(HOST, PORT);

        // Process the stream
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(Integer::sum);

        wordCounts.foreachRDD(rdd -> {
            rdd.collect().forEach(System.out::println);
        });

        // Start Spark Streaming
        try {
            jssc.start();
            jssc.awaitTermination();
        } catch (Exception e) {
            System.err.println("ERROR: An unexpected error occurred - " + e.getMessage());
            e.printStackTrace();
        } finally {
            jssc.close();
        }
    }

    /**
     * Checks if the given host and port are open.
     */
    private static boolean isPortOpen(String host, int port, int timeout) {
        try (Socket socket = new Socket(host, port)) {
            return true;
        } catch (UnknownHostException e) {
            System.err.println("ERROR: Unknown host " + host);
        } catch (IOException e) {
            return false; // Port is closed or unreachable
        }
        return false;
    }
}