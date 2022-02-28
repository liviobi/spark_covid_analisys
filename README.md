# Analysis of COVID-19 Data with Spark

In this project I analyze the evolution of the COVID-19 situation worldwide starting from an open [dataset](https://www.ecdc.europa.eu/en/publications-data/download-todays-data-geographic-distribution-covid-19-cases-worldwide) reporting the new cases for each country.
The dataset has the following structure.

| date | ... | cases | deaths | country | ... |
| ---- | --- | ----- | ------ | ------- | --- |

The queries computed are the following

1. Seven days moving average of new reported cases, for each county and for each day
2. Percentage increase (with respect to the day before) of the seven days moving average, for each country
   and for each day
3. Top 10 countries with the highest percentage increase of the seven days moving average, for each day

## Choiche of the framework

This project was done as an excercise on Spark. I decided to use batch processing since I was dealing with historical data and between regual Spark and SparkSQL I decided to use the latter because it was sufficient to compute all the queries having the advantage to provide a greter level of abstraction

## Runnig locally on unix

1. Download Apache Spark [here](https://spark.apache.org/downloads.html)
2. Append this commands to `~/ .bashrc`

   ```bash
   export SPARK_LOCAL_IP=127.0.0.1
   export SPARK_MASTER_HOST=127.0.0.1
   ```

3. Create a tmp log folder for Spark with `mkdir -p /tmp/spark-events`
4. Navigate to the Apache Spark’s folder with cd `/path/to/spark/folder` and edit `conf/spark-defaults.conf` with the following configurations:

   ```
   spark.master                     spark://127.0.0.1:7077
   spark.eventLog.enabled           true
   spark.eventLog.dir               /tmp/spark-events
   ```

5. Start the master from Spark's folder with `./sbin/start-master.sh` <br>
   Check if there are no errors by going to http://localhost:8080
6. Start the slave running `./sbin/start-slave.sh`
   <br> At this point you should be able to connect to http://localhost:8081/ and see the slave running.
7. Build the jar of the project using maven
8. Submit an executor using
   <br>
   `./bin/spark-submit --class [classhpath] [artifact-path] spark://127.0.0.1:7077 [output_folder]`
   <br>
   For example in my case the command looks this way:
   <br>`./bin/spark-submit --class it.polimi.middleware.spark.batch.covid.Covid_Analysis /home/livio/IdeaProjects/spark_covid_analisys/target/spark_covid_analisys-1.0.jar spark://127.0.0.1:7077 /home/livio/IdeaProjects/spark_covid_analisys/`
9. If you want to see the events that occurred during execution: ./sbin/start-history-server.sh and than connect from the browser to http://127.0.0.1:18080/

## Runnnig on Amazon EMR

1. Build the jar of the project using maven
2. Create a s3 bucket for the project and put the input file and the jar of the project
3. Go to EMR and create a cluster (explained well in [this](https://www.youtube.com/watch?v=An8tw4lEkaI&t=192s) youtube video)
4. Open a terminal on your local machine and ssh to the cluster
5. Copy the jar file from s3 to the cluster with
   <br>`aws s3 cp s3://[path_to_the_jar] .`<br>
   For example
   <br>
   `aws s3 cp s3://polimimiddlewareproject/spark_covid_analisys-1.0.jar .`
6. Submit the job to the cluster with<br>
   `spark-submit --class [classpath] [artifact_path] [output_folder]`<br>
   For example<br>
   `spark-submit --class it.polimi.middleware.spark.batch.covid.Covid_Analysis ./spark_covid_analisys-1.0.jar yarn-client s3://polimimiddlewareproject/`
