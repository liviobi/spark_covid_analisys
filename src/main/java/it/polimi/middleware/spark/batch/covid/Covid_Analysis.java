package it.polimi.middleware.spark.batch.covid;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import it.polimi.middleware.spark.utils.LogUtils;

import static org.apache.spark.sql.functions.*;


public class Covid_Analysis {
    private static final boolean useCache = true;

    public static void main(String[] args) {
        LogUtils.setLogLevel();

        final String master = args.length > 0 ? args[0] : "local[4]";
        final String filePath = args.length > 1 ? args[1] : "./";
        //final String appName = useCache ? "BankWithCache" : "BankNoCache";

        final SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName("Covid Analysis")
                .getOrCreate();

//
        Dataset<Row> cases_per_country_df = spark
                .read()
                .option("header", "true")
                .option("delimiter", ",")
                .csv(filePath + "files/covid/data.csv");


        cases_per_country_df = cases_per_country_df.select( col("dateRep"),
                                        to_date(col("dateRep"), "dd/MM/yyyy").as("date"),
                                        to_timestamp(col("dateRep"), "dd/MM/yyyy").as("timestamp"),
                                        col("countriesAndTerritories").as("country"),
                                        col("cases").cast(DataTypes.IntegerType));

        long days = 6 * 86400;

        cases_per_country_df = cases_per_country_df
                                .withColumn("moving_avg", avg("cases")
                                .over( Window.partitionBy("country").orderBy(col("timestamp").cast("long")).rangeBetween(-days, 0)));

        //calcola dato aggregato
        Dataset<Row>  daily_cases_df = cases_per_country_df
                                        .groupBy("date","timestamp")
                                        .sum("cases")
                                        .orderBy("date")
                                        .withColumnRenamed("sum(cases)","cases");
        daily_cases_df = daily_cases_df.withColumn("moving_avg", avg("cases")
                .over( Window.orderBy(col("timestamp").cast("long")).rangeBetween(-days, 0)));

        daily_cases_df = daily_cases_df
                                .withColumn("shifted_moving_avg",lag("moving_avg",1)
                                .over(Window.orderBy("timestamp")));
        daily_cases_df = daily_cases_df.withColumn("percentage_increase",
                when(
                        isnull(col("shifted_moving_avg")), 0)
                        .otherwise(
                                (col("shifted_moving_avg")
                                        .minus(col("moving_avg"))
                                        .divide(col("moving_avg")))
                        )
        );
        daily_cases_df = daily_cases_df.select("date","cases","moving_avg","percentage_increase");


        daily_cases_df
                .coalesce(1)
                .write()
                .option("header", "true")
                .mode("overwrite")
                .csv(filePath + "files/covid/output/daily_cases.csv");

        daily_cases_df.show();


        WindowSpec window = Window.partitionBy("country").orderBy("timestamp");
        cases_per_country_df = cases_per_country_df.withColumn("shifted_moving_avg",lag("moving_avg",1).over(window));
        //rimuovi righe che contengono null values

        cases_per_country_df = cases_per_country_df.withColumn("percentage_increase",
                                                    when(
                                                            isnull(col("shifted_moving_avg")), 0)
                                                    .otherwise(
                                                            (col("shifted_moving_avg")
                                                            .minus(col("moving_avg"))
                                                            .divide(col("moving_avg")))
                                                    )
        );
        cases_per_country_df = cases_per_country_df.select("date","country","cases","moving_avg","percentage_increase");

        cases_per_country_df
                .coalesce(1)
                .write()
                .option("header", "true")
                .mode("overwrite")
                .csv(filePath + "files/covid/output/cases_per_country.csv");

        cases_per_country_df.show();

        window = Window.partitionBy("date").orderBy(desc("percentage_increase"));
        //select only meaningful data
        Dataset<Row> top_ten_df = cases_per_country_df.select(cases_per_country_df.col("*"), rank().over(window).alias("rank"))
                                            .filter(col("rank").leq(10))
                                            .orderBy("date","rank");
        top_ten_df = top_ten_df.select("date","country","percentage_increase","rank");
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