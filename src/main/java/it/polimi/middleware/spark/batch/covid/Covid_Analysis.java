package it.polimi.middleware.spark.batch.covid;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;

import it.polimi.middleware.spark.utils.LogUtils;

import static org.apache.spark.sql.functions.*;


public class Covid_Analysis {

    public static void main(String[] args) {
        LogUtils.setLogLevel();

        //Pass spark://127.0.0.1:7077 when running the local machine
        //Pass yarn-client when running on aws
        final String master = args.length > 0 ? args[0] : "local[4]";

        final String filePath = args.length > 1 ? args[1] : "./";


        final SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName("Covid Analysis")
                .getOrCreate();

        Dataset<Row> cases_per_country_df = spark
                .read()
                .option("header", "true")
                .option("delimiter", ",")
                .csv(filePath + "files/covid/data.csv");

        //Need to have the data in timestamp format in order to be able to use the rangeBetween function
        cases_per_country_df = cases_per_country_df.select( col("dateRep"),
                                        to_date(col("dateRep"), "dd/MM/yyyy").as("date"),
                                        to_timestamp(col("dateRep"), "dd/MM/yyyy").as("timestamp"),
                                        col("countriesAndTerritories").as("country"),
                                        col("cases").cast(DataTypes.IntegerType))
                                        .cache()
        ;
        //86400 are the second in a day. Need this to compute the beginning of the window
        long days = 6 * 86400;

        //For each country compute the 7 days moving average of the cases. I want to use the function rangeBetween() because
        //it assures that the window is 7 days, so it's safe in case of missing rows unlike rowsBetween() i.e. I don't risk to
        //take into consideration rows older than a week
        cases_per_country_df = cases_per_country_df
                                .withColumn("moving_avg", avg("cases")
                                .over( Window.partitionBy("country").orderBy(col("timestamp").cast("long")).rangeBetween(-days, 0)))
                                .cache()
        ;

        /**
         * DAILY CASES
         */

        //Compute the cases for each day
        Dataset<Row>  daily_cases_df = cases_per_country_df
                                        .groupBy("date","timestamp")
                                        .sum("cases")
                                        .orderBy("date")
                                        .withColumnRenamed("sum(cases)","cases")
                                        .cache()
        ;


        //compute the 7 days moving average
        daily_cases_df = daily_cases_df
                                        .withColumn("moving_avg", avg("cases")
                                        .over( Window.orderBy(col("timestamp").cast("long")).rangeBetween(-days, 0)))
                                        .cache()
        ;

        //Add a column with the moving average for the previous day
        daily_cases_df = daily_cases_df
                                        .withColumn("prev_day_moving_average",lag("moving_avg",1)
                                        .over(Window.orderBy("timestamp")))
                                        .cache()
        ;

        //compute the percentage increase
        daily_cases_df = daily_cases_df.withColumn("percentage_increase",
                when(
                        isnull(col("prev_day_moving_average")), 0)
                .otherwise(
                        col("moving_avg")
                                .minus(col("prev_day_moving_average"))
                                .divide(col("moving_avg").plus(1))
                        )
        )
                .cache()
        ;

        daily_cases_df = daily_cases_df
                                        .select("date","cases","moving_avg","percentage_increase")
        ;

        //save to file
        daily_cases_df
                .coalesce(1)
                .write()
                .option("header", "true")
                .mode("overwrite")
                .csv(filePath + "files/covid/output/daily_cases.csv");

        daily_cases_df.show();
        //Won't need this data in the future, unpersist
        daily_cases_df.unpersist();


        /**
         * CASES PER COUNTRY
         */
        //I need to consider each country independently for computing the moving average percentage increase
        WindowSpec window = Window.partitionBy("country").orderBy("timestamp");
        //Add a column with the moving average for the previous day
        cases_per_country_df = cases_per_country_df
                                                    .withColumn("prev_day_moving_average",lag("moving_avg",1)
                                                    .over(window))
                                                    .cache()
        ;

        //compute the percentage increase
        cases_per_country_df = cases_per_country_df
                                                    .withColumn("percentage_increase",
                                                        when(
                                                                isnull(col("prev_day_moving_average")), 0)
                                                        .otherwise(
                                                                (col("moving_avg")
                                                                        .minus(col("prev_day_moving_average"))
                                                                        .divide(col("moving_avg").plus(1)))
                                                        )
                                                    )
                                                    .cache()
        ;


        cases_per_country_df = cases_per_country_df
                                                    .select("date", "country", "cases", "moving_avg", "percentage_increase")
        ;

        cases_per_country_df
                .coalesce(1)
                .write()
                .option("header", "true")
                .mode("overwrite")
                .csv(filePath + "files/covid/output/cases_per_country.csv");

        cases_per_country_df.show();


        /**
         * TOP 10 COUNTRIES WITH GREATER PERCENTAGE INCREASE
         */
        window = Window
                        .partitionBy("date")
                        .orderBy(desc("percentage_increase"));

        //Rank the rows based on the percentage increase and then take the one with rank less than equal 10
        Dataset<Row> top_ten_df = cases_per_country_df
                                            //.na()
                                            //.drop()
                                            .select(cases_per_country_df.col("*"), rank().over(window).alias("rank"))
                                            .filter(col("rank").leq(10))
                                            .orderBy("date","rank")
                                            .cache()
        ;
        top_ten_df = top_ten_df
                                .select("date","country","percentage_increase","rank")
        ;

        top_ten_df
                .coalesce(1)
                .write()
                .option("header", "true")
                .mode("overwrite")
                .csv(filePath + "files/covid/output/top_ten.csv");
        top_ten_df.show();

        spark.close();

    }
}