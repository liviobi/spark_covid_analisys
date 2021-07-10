package it.polimi.middleware.spark.batch.covid;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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
        Dataset<Row> withdrawals = spark
                .read()
                .option("header", "true")
                .option("delimiter", ",")
                //.schema(mySchema)
                .csv(filePath + "files/covid/data.csv");

        withdrawals = withdrawals.withColumn("cases",col("cases").cast(DataTypes.IntegerType));


        // Q1. Total amount of withdrawals for each person
        Dataset<Row> totAmount = withdrawals
                .groupBy("countryterritoryCode")
                .sum("cases")
                .withColumnRenamed("sum(cases)","casi");
        totAmount.show();

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