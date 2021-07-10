package it.polimi.middleware.spark.batch.iterative;

import it.polimi.middleware.spark.utils.LogUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.Arrays;
import java.util.concurrent.atomic.LongAccumulator;

/**
 * Start from a dataset of investments. Each element is a Tuple2(amount_owned, interest_rate).
 * At each iteration the new amount is (amount_owned * (1+interest_rate)).
 *
 * Implement an iterative algorithm that computes the new amount for each investment and stops
 * when the overall amount overcomes 1000.
 */
public class InvestmentSimulator {
    public static void main(String[] args) {
        LogUtils.setLogLevel();

        final String master = args.length > 0 ? args[0] : "local[1]";
        final String filePath = args.length > 1 ? args[1] : "./";
        final double threshold = 1000;

        final SparkConf conf = new SparkConf().setMaster(master).setAppName("InvestmentSimulator");
        final JavaSparkContext sc = new JavaSparkContext(conf);

        final JavaRDD<String> textFile = sc.textFile(filePath + "files/iterative/investment.txt");

        int iteration = 0;
        double sum = 0;

        // scritto da me
        JavaPairRDD<Double, Double> investments = textFile.mapToPair(
                line-> new Tuple2<>(
                        Double.parseDouble(Arrays.asList(line.split(" ")).get(0)),
                        Double.parseDouble(Arrays.asList(line.split(" ")).get(1)) + 1
                )
        );

        System.out.println(investments.collect());

        do{
            iteration ++;
            investments = investments.mapToPair(t-> new Tuple2<>(t._1*t._2,t._2));
            sum = investments.map(t-> t._1).reduce(Double::sum);

            System.out.println(investments.collect());
            System.out.println("Sum: " + sum + " after " + iteration + " iterations");
            }while (sum < threshold);


        // scritto da me

        //System.out.println("Sum: " + sum + " after " + iteration + " iterations");
        sc.close();
    }

}
