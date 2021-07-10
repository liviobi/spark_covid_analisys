package it.polimi.middleware.spark.batch.wordcount;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import it.polimi.middleware.spark.utils.LogUtils;
import scala.Tuple2;


public class WordCount {

    public static void main(String[] args) {
        LogUtils.setLogLevel();
        //local 4 = use the local to say to launch 4 threads in the local jvm
        final String master = args.length > 0 ? args[0] : "local[4]";
        final String filePath = args.length > 1 ? args[1] : "./";

        final SparkConf conf = new SparkConf().setMaster(master).setAppName("WordCount");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        final JavaRDD<String> lines = sc.textFile(filePath + "files/wordcount/in.txt");

        //for each line split it into words . .
        final JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        //create a key value pair
        final JavaPairRDD<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
        //reduce
        final JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);
        //action, must execute to start the computation
        System.out.println(counts.collect());
        sc.close();
    }

}