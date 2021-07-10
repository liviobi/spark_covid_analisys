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
public class InvestmentSimulator2 {
    //questa variabile serve per contare quante volte viene eseguito accumulate interest, senza ottimizzazioni spark
    //ogni volta che che fa un iterazione si ricalcola tutti i dati dall'inizio, NB la variabile count
    // funziona solo in locale non funzionerebbe in ambiente distribuito perchè ogni worker avrebbe la sua copia
    static  int count = 0;


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

        // scritto da collega
        JavaPairRDD<Double, Double> investments = textFile.mapToPair(InvestmentSimulator2::parseLine).cache();
        JavaPairRDD<Double, Double> oldInvestments = investments;
        sum = investments.map(t-> t._1).reduce(Double::sum);

        while(sum < threshold){
            //commentare .cache per vedere come aumenta il numero delle chiamate ad accumulate interest
            //perchè praticamente senza caching si calcola ogni volta tutte le operazioni dall'inizio
            investments = investments.mapToPair(InvestmentSimulator2::accumulateInterest).cache();

            sum = investments.map(t-> t._1).reduce(Double::sum);
            //optimization clear the RDD cached in the previous iteration
            oldInvestments.unpersist();
            oldInvestments = investments;

            /* map to pair of investments is lazy evaluated so it gets executed only
               when sum is called, while unpersist is eager so it gets called right away,
               therefore cache gets cleared when it's still useful

               WRONG!!

            oldInvestments.unpersist();
            oldInvestments = investments;
            sum = investments.map(t-> t._1).reduce(Double::sum);*/

            System.out.println("iteration: " + iteration );
            iteration ++;
        }

        investments.unpersist();

        System.out.println("Sum: " + sum + " after " + iteration + " iterations");


        // scritto da collega

        //System.out.println("Sum: " + sum + " after " + iteration + " iterations");
        sc.close();
    }



    private static <K2, V2> Tuple2<Double,Double> parseLine(String l) {
            String[] split = l.split(" ");
            return new Tuple2<>(Double.parseDouble(split[0]),Double.parseDouble(split[1]));
    }

    //viene eseguita una volta per tupla, dunque nel caso in esempio 5 volte per iterazione
    private static <K2, V2> Tuple2<Double,Double> accumulateInterest(Tuple2<Double, Double> t) {
        //just to see how many times it gets invoked
        System.out.println("Execute accumulate interest "+ (count++));
        return  new Tuple2<>(t._1*(1 + t._2), t._2);
    }

}
