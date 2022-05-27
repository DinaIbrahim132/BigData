// Databricks notebook source


// COMMAND ----------

val Data = spark
  .read
  .option("inferSchema", "true")
  .option("header", "true")
  .csv("/FileStore/tables/assignment2/by-day")
Data.count()

// COMMAND ----------

val dataSchema = Data.schema

// COMMAND ----------

/* Setting up a schema that reads 20 file per trigger */
val streaming = spark
                  .readStream.schema(dataSchema)
                  .option("maxFilesPerTrigger", 20)
                  .csv("/FileStore/tables/ass2/by-day")

// COMMAND ----------

import org.apache.spark.sql.functions.{col, sum}
val x= streaming
                .withColumn("value",col("Quantity")*col("UnitPrice"))
                .groupBy("CustomerID").agg(sum("value").alias("total value"),sum("Quantity").alias("total stocks"))


// COMMAND ----------

val sqlWayQuery = x.writeStream.queryName("s")
  .format("memory").outputMode("complete")
  .start()


// COMMAND ----------

/* Setting the shuffle partition limit allows you to limit the level of paritioning. Why do this? */

spark.conf.set("spark.sql.shuffle.partitions", 5)

// COMMAND ----------

sqlWayQuery.awaitTermination()

// COMMAND ----------

spark.streams.active


// COMMAND ----------

for( i <- 1 to 20 ) {
    spark.sql("SELECT * FROM s").show()
    Thread.sleep(1000)
}


// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

stocksQuery.awaitTermination()


// COMMAND ----------

valueQuery.awaitTermination()


// COMMAND ----------

/* Querying for the active streams under our Spark session */
spark.streams.active

// COMMAND ----------

/* 
  activity_counts is a table so we can use sparkSql to query it.
  So we can re-run the query every second to see the progression.
 */
for( i <- 1 to 1 ) {
    spark.sql("SELECT * FROM stocks_counts").show()
    Thread.sleep(1000)
}


// COMMAND ----------

/* 
  activity_counts is a table so we can use sparkSql to query it.
  So we can re-run the query every second to see the progression.
 */
for( i <- 1 to 1 ) {
    spark.sql("SELECT * FROM value_sums").show()
    Thread.sleep(1000)
}

// COMMAND ----------

1+1

// COMMAND ----------


