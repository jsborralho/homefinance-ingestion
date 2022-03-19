package pt.homefinance.ingestion

import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

import java.io.File

trait SharedSparkSession extends BeforeAndAfterAll {

  self: Suite =>

  @transient private var _spark: SparkSession = _
  @transient private lazy val conf = {
    new SparkConf(false)
      .set("spark.ui.enabled", "false")
      .set("spark.testing.memory", "2147480000")
  }

  def sparkSession: SparkSession = _spark

  def sparkContext: SparkContext = _spark.sparkContext

  def sqlContext: SQLContext = _spark.sqlContext


  def clearAll(): Unit = {
    clearSparkCaches()
  }

  def clearSparkCaches(): Unit = {
    _spark.sqlContext.clearCache()
    _spark.sparkContext.getPersistentRDDs.foreach(_._2.unpersist(blocking = true))
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    val metastoreDir = new File("metastore_db")
    val warehouseDir = new File("spark-warehouse")

    if (metastoreDir.exists()) JavaUtils.deleteRecursively(metastoreDir)
    if (warehouseDir.exists()) JavaUtils.deleteRecursively(warehouseDir)

    System.setProperty("hadoop.home.dir", s"${System.getProperty("user.dir")}/src/test/resources/hadoop-gil")

    _spark = SparkSession.builder
      .config(conf)
      .appName("test")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    _spark.sparkContext.setLogLevel("WARN")
    _spark.sparkContext.setCheckpointDir("/tmp")
  }

  override def afterAll(): Unit = {
    super.afterAll()
    _spark.stop()
    _spark = null
    val metastoreDir = new File("metastore_db")
    val warehouseDir = new File("spark-warehouse")

    if (metastoreDir.exists()) JavaUtils.deleteRecursively(metastoreDir)
    if (warehouseDir.exists()) JavaUtils.deleteRecursively(warehouseDir)
  }

}
