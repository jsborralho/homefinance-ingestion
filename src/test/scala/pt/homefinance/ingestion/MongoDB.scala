package pt.homefinance.ingestion

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class MongoDB extends FunSuite{


  test("connect to mongo") {
    val uri = "mongodb://test:password@localhost/PersonalFinance.Home"
    val spark = SparkSession
      .builder()
      .appName("Mongo-Test-Upload")
      .master("local[*]")
      .config("spark.mongodb.input.uri", uri)
      .config("spark.mongodb.output.uri", uri)
      .getOrCreate()

    val df = MongoSpark.load(spark)  // Uses the SparkSession

    df.printSchema()                        // Prints DataFrame schema


  }
}
