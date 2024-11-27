package KMeans

import org.apache.spark.sql.SparkSession

// note: Must set up project with Java 8 and SDK 8 (for compatibility with this spark version)

object KMeans {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("KMeans").master("local[*]").getOrCreate()
    println("spark session successfully started.")
  }
}
