package hw4

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable


object Main {
  def getFreq(text: String): mutable.HashMap[String, Double] = {
    val textSplit = text.split(" ")
    val freq = 1 / textSplit.length.toDouble

    val wordFreq = new mutable.HashMap[String, Double]()
    for (s <- textSplit) {
      if (wordFreq.contains(s))
        wordFreq(s) += freq
      else
        wordFreq(s) = freq
    }
    wordFreq
  }

  def main(args: Array[String]): Unit = {
    val dataPath = args(0)
    println(s"Got data path: $dataPath")

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("hw4")
      .getOrCreate()
    import spark.implicits._
    println(s"Session created")

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(dataPath)
    val totalDocs = df.count()
    println(s"Got dataset with length: $totalDocs")

    val dfProcessed = df.withColumn(
      "textProcessed",
      regexp_replace(lower(col("Review")), """[\p{Punct}]""", "")
    )
    println("Text processed")
    println(dfProcessed.select("textProcessed").show(5, truncate=100, vertical=true))

    val wordTF = dfProcessed
      .select("textProcessed")
      .map(x => getFreq(x(0).toString))
      .withColumn("id", monotonically_increasing_id())
      .select(col("id"), explode(col("value")).as(Array("word", "TF")))

    val topWordsWithIDF = wordTF
      .groupBy(col("word"))
      .agg(count(col("id")) as "count")
      .orderBy(desc("count"))
      .withColumn("IDF", log(lit(totalDocs) / col("count")))
      .limit(100)

    val topWordsTFIDF = wordTF
      .join(topWordsWithIDF, "word")
      .withColumn("TFIDF", col("TF") * col("IDF"))
      .select(col("id"), col("word"), col("TFIDF"))
    println("TFIDF calculated")

    val result = topWordsTFIDF
      .groupBy("id")
      .pivot(col("word"))
      .sum("TFIDF")
      .na.fill(0)
      .orderBy("id")
    println(result.show())

    spark.stop()
  }
}
