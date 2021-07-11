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

/**
 * Bank example
 *
 * Input: csv files with list of deposits and withdrawals, having the following
 * schema ("person: String, account: String, amount: Int)
 *
 * Queries
 * Q1. Print the total amount of withdrawals for each person.
 * Q2. Print the person with the maximum total amount of withdrawals
 * Q3. Print all the accounts with a negative balance
 */
public class Covid_Analysis {
    private static final boolean useCache = true;

    public static void main(String[] args) {
        LogUtils.setLogLevel();

        final String master = args.length > 0 ? args[0] : "local[4]";
        final String filePath = args.length > 1 ? args[1] : "./";
        final String appName = useCache ? "BankWithCache" : "BankNoCache";

        final SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName("Bank")
                .getOrCreate();

        //final List<StructField> mySchemaFields = new ArrayList<>();
        //mySchemaFields.add(DataTypes.createStructField("person", DataTypes.StringType, true));
        //mySchemaFields.add(DataTypes.createStructField("account", DataTypes.StringType, true));
        //mySchemaFields.add(DataTypes.createStructField("amount", DataTypes.IntegerType, true));
        //final StructType mySchema = DataTypes.createStructType(mySchemaFields);

        //final Dataset<Row> deposits = spark
        //        .read()
        //        .option("header", "false") //there is no header, read the entire file
        //        .option("delimiter", ",")
        //        .schema(mySchema) //I'm providing the schema since there's no header(person, account amount)
        //        .csv(filePath + "files/bank/deposits.csv");
//
        Dataset<Row> covid_data = spark
                .read()
                .option("header", "true")
                .option("delimiter", ",")
                //.option("inferSchema","true")
                //.schema(mySchema)
                .csv(filePath + "files/covid/data.csv");
        long days = 6 * 86400;
        //covid_data = covid_data.withColumn("cases",col("cases").cast(DataTypes.IntegerType));
        covid_data = covid_data.select( col("dateRep"),
                                        to_date(col("dateRep"), "dd/MM/yyyy").as("date"),
                                        to_timestamp(col("dateRep"), "dd/MM/yyyy").as("timestamp"),
                                        col("countriesAndTerritories").as("country"),
                                        col("cases").cast(DataTypes.IntegerType));

        covid_data = covid_data
                                .withColumn("moving_avg", avg("cases")
                                .over( Window.partitionBy("country").orderBy(col("timestamp").cast("long")).rangeBetween(-days, 0)));

        //calcola dato aggregato
        Dataset<Row>  daily_cases = covid_data
                                        .groupBy("date","timestamp")
                                        .sum("cases")
                                        .orderBy("date")
                                        .withColumnRenamed("sum(cases)","cases");
        daily_cases = daily_cases.withColumn("moving_avg", avg("cases")
                .over( Window.orderBy(col("timestamp").cast("long")).rangeBetween(-days, 0)));

        daily_cases = daily_cases
                                .withColumn("shifted_moving_avg",lag("moving_avg",1)
                                .over(Window.orderBy("timestamp")));
        daily_cases = daily_cases.withColumn("percentage_increase",
                when(
                        isnull(col("shifted_moving_avg")), 0)
                        .otherwise(
                                (col("shifted_moving_avg")
                                        .minus(col("moving_avg"))
                                        .divide(col("moving_avg")))
                        )
        );
        daily_cases = daily_cases.select("date","cases","moving_avg","percentage_increase");
        daily_cases.show();


        WindowSpec window = Window.partitionBy("country").orderBy("timestamp");
        covid_data = covid_data.withColumn("shifted_moving_avg",lag("moving_avg",1).over(window));
        //rimuovi righe che contengono null values

        covid_data = covid_data.withColumn("percentage_increase",
                                                    when(
                                                            isnull(col("shifted_moving_avg")), 0)
                                                    .otherwise(
                                                            (col("shifted_moving_avg")
                                                            .minus(col("moving_avg"))
                                                            .divide(col("moving_avg")))
                                                    )
        );

        window = Window.partitionBy("date").orderBy(desc("percentage_increase"));
        //select only meaningful data
        Dataset<Row> top_ten = covid_data.select(covid_data.col("*"), rank().over(window).alias("rank"))
                                            .filter(col("rank").leq(10))
                                            .orderBy("date","rank");
        top_ten = top_ten.select("date","percentage_increase","rank");
        top_ten
                .coalesce(1)
                .write()
                .option("header", "true")
                .csv(filePath + "files/covid/output/top_ten.csv");
        //top_ten.show();


        //Increase = (New Number - Original Number) /  original

        /*Dataset<Row> totAmount = covid_data
                .groupBy("country")
                .sum("cases")
                .withColumnRenamed("sum(cases)","casi");
        totAmount.show();*/

        /*//cast cases to int
        withdrawals = withdrawals.withColumn("cases",col("cases").cast(DataTypes.IntegerType));
        //withdrawals.createOrReplaceTempView("withdrawals");
        //spark.sql("SELECT TO_DATE(CAST(UNIX_TIMESTAMP(dateRep, 'dd/MM/yyyy') AS TIMESTAMP)) AS newdate FROM withdrawals").show();
        withdrawals = withdrawals.withColumn("movingAverage", avg("cases")
                .over( Window.partitionBy("countriesAndTerritories").rowsBetween(-6,0)));
        withdrawals.show();*/

        //val wSpec1 = Window.partitionBy("name").orderBy("date").rowsBetween(-2, 0)

        /* SQL QUERY PROGRAMMATICALLY
        // Register the DataFrame as a SQL temporary view
        withdrawals.createOrReplaceTempView("withdrawals");
        Dataset<Row> sqlDF = spark.sql("SELECT person, sum(amount) as TotalAmount FROM withdrawals GROUP BY person");
        sqlDF.show();*/



       /* // Q2. Person with the maximum total amount of withdrawals
        totAmount.orderBy(desc("TotalAmount")).limit(1).show();
        // Q3 Accounts with negative balance
        Dataset<Row> withdrawalsByAccount = withdrawals
                .groupBy("person","account")
                .sum("amount")
                .withColumnRenamed("sum(amount)","total1" );
        Dataset<Row> depositsByAccount = deposits
                .groupBy("person","account")
                .sum("amount")
                .withColumnRenamed("sum(amount)","total2" );
        withdrawalsByAccount.join(depositsByAccount,
                withdrawalsByAccount.col("person").equalTo(depositsByAccount.col("person"))
                        .and(withdrawalsByAccount.col("account").equalTo(depositsByAccount.col("account"))),
                "left_outer").na().fill(0)
                .filter(col("total1").gt(col("total2")))
                .show();*/
        spark.close();

    }
}