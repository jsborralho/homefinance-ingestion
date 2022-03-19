package pt.homefinance.ingestion

import com.mongodb.spark.MongoSpark
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source


object IngestionApp extends LazyLogging{

  def main(args: Array[String]): Unit = {
    logger.info("Start ingesting data")

    val uri = "mongodb://test:password@localhost/PersonalFinance.Home"

    val spark = SparkSession
      .builder()
      .appName("Mongo-Test-Upload")
      .master("local[*]")
      .config("spark.mongodb.input.uri", uri)
      .config("spark.mongodb.output.uri", uri)
      .getOrCreate()

    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(getClass.getClassLoader.getResource("").getPath)

    //df.show(100)

    MongoSpark.save(df.write.option("collection", "Home").mode("overwrite"))

  }
}
