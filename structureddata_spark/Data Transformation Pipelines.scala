// Databricks notebook source
/*
Loading this data file using the SparkContext will return RDD data APIs.
*/
val textFile = spark.sparkContext.textFile("/FileStore/tables/Text Corpus/*.text")


// COMMAND ----------

/* Count the number of lines across all the files. */
textFile.count()

// COMMAND ----------

/* Find the number of occurrences of the word “antibiotics */
val antibioticsCount = textFile.filter(line => line.contains("antibiotics"))
antibioticsCount.count()

// COMMAND ----------

/* showing all the lines that contains the word “antibiotics */
val antibioticDF = antibioticsCount.toDF()
antibioticDF.show(false)

// COMMAND ----------

/* Count the occurrence of the word “patient” and “admitted” 
on the same line of text using two transformation functions */

val twoWordsOccurrences = textFile.filter(line => line.contains("patient"))
                                  .filter(line => line.contains("admitted"))
twoWordsOccurrences.count()

// COMMAND ----------

/* showing all lines contains the word “patient”
and “admitted” on the same line of text */

val twoWordsDF = twoWordsOccurrences.toDF()
twoWordsDF.show(false)
