package KMeans

import org.apache.spark.sql.SparkSession

import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._
import scala.util.Random

// note: Must set up project with Java 8 and SDK 8 (for compatibility with this spark version)

object KMeans {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KMeans").setMaster("local[*]")
    val sc = new SparkContext(conf)
    println("spark session successfully started.")

    // air_quality_normalized.csv, population_density_normalized.csv
    // read in data and execute join on city name
    val air_quality_records = sc.textFile("air_quality_normalized.csv").map(line => line.split(","))
      .map(fields => (fields(0), fields.tail))
    val population_density_records = sc.textFile("population_density_normalized.csv").map(line => line.split(","))
      .map(fields => (fields(0), fields.tail))
    // set map of city name -> feature vector (list or tuple)
    val records = air_quality_records.join(population_density_records).map {
      case (city, (values1, values2)) => (city, values1 ++ Array(values2(2)))}

    records.collect().foreach { case (city, featureVector) =>
      println(s"City: $city, Features: ${featureVector.mkString("[", ", ", "]")}, featureCount: ${featureVector.count(p => true)}")
    }

    // set hyperparameters
    // k - number of clusters
    val k = 3
    // centroids - initial k centroids - determine randomly
    val rand = new Random
    val centroids = (1 to k).map(_ => rand.nextInt(records.count().toInt))
    // convergence threshold -
    val convThresh = 0.5

    // assign points to clusters - each cluster is a map of centroid vector -> list of cluster city names
    // calculate euclidean distance between point and each centroid

    // recompute centroids

    // check for convergence, break out of loop if convergence threshold is met

    // after loop ends
    // print the cities for each cluster, with their vectors
  }
}
