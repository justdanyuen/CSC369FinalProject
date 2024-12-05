package KMeans

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._

import scala.io._
import java.io._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
//import org.apache.log4j.Logger
//import org.apache.log4j.Level
//import org.apache.spark.sql.catalyst.dsl.expressions.longToLiteral

import scala.collection._
import scala.util.Random
import scala.math.sqrt

// note: Must set up project with Java 8 and SDK 8 (for compatibility with this spark version)

object KMeans {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:/winutils/")
    val conf = new SparkConf().setAppName("KMeans").setMaster("local[*]")
    val sc = new SparkContext(conf)
    println("spark session successfully started.")

    // air_quality_normalized.csv, population_density_normalized.csv
    // read in data and execute join on city name
    val air_quality_records = sc.textFile("air_quality_normalized.csv")
      .zipWithIndex().filter { case (_, index) => index > 0 }
      .map { case (line, _) => line.split(",") }
      .map(fields => (fields(0), fields.tail.map(_.toDouble)))
    val population_density_records = sc.textFile("population_density_normalized.csv")
      .zipWithIndex().filter { case (_, index) => index > 0 }
      .map { case (line, _) => line.split(",") }
      .map(fields => (fields(0), fields.tail.map(_.toDouble)))
    // set map of city name -> feature vector (list or tuple)
    val records = air_quality_records.join(population_density_records).map {
      case (city, (values1, values2)) => (city, values1.map(_.toDouble) ++ Array(values2(2).toDouble))}

    records.collect().foreach { case (city, featureVector) =>
      println(s"City: $city, Features: ${featureVector.mkString("[", ", ", "]")}, featureCount: ${featureVector.count(p => true)}")
    }

    // set hyperparameters
    // k - number of clusters
    // 4 ended up being most optimal
    val k = 4
    // centroids - initial k centroids - determine randomly
    val rand = new Random
    val centroids = (1 to k).map(_ => rand.nextInt(records.count().toInt)).toArray
    // convergence threshold -
    val convThresh = 1

    val indexedRecords = records.zipWithIndex()
    indexedRecords.collect().foreach { case (city, featureVector) =>
      println(s"City: $city, Feature vector type: ${featureVector.getClass}")
    }
    // get array of centroid vectors
    val centroidVectors = centroids.map { i =>
      indexedRecords.filter { case (_, index) => index == i }
        .map { case ((_, featureVector), _) => featureVector }
        .first()
    }

    // get map (centroid vector, array of city names)
    val clusterMap = kMeans(records, centroidVectors, convThresh)
    clusterMap.foreach { case (centroidVector, cityArray) =>
      println(s"Centroid: ${centroidVector.mkString("[", ", ", "]")}, Cities: ${cityArray.mkString(", ")}")
    }

    println("\nFinal Clustering Results:")
    println("========================")
    clusterMap.collect().zipWithIndex.foreach { case ((centroid, cities), index) =>
      println(s"\nCluster ${index + 1}")
      println(s"Centroid: ${centroid.mkString("[", ", ", "]")}")
      println("Cities in this cluster:")
      cities.foreach { city =>
        val cityFeatures = records.lookup(city).head
        println(s"  $city: ${cityFeatures.mkString("[", ", ", "]")}")
      }
      println("------------------------")
    }

  }


  // Add this function to calculate WCSS
  def calculateWCSS(centroidsToCitiesRDD: RDD[(Array[Double], Array[String])], points: RDD[(String, Array[Double])]): Double = {
    val pointsMap = points.collectAsMap()
    val pointsBroadcast = points.context.broadcast(pointsMap)

    // Sum of squared distances for each cluster
    val wcss = centroidsToCitiesRDD.map { case (centroid, cities) =>
      val pointsMap = pointsBroadcast.value
      cities.map { city =>
        val cityVector = pointsMap.getOrElse(city, Array.fill(centroid.length)(0.0))
        euclideanDistance(cityVector, centroid) * euclideanDistance(cityVector, centroid)
      }.sum // Sum of squared distances for this cluster
    }.sum() // Total WCSS for all clusters

    wcss
  }

  def kMeans(points: RDD[(String, Array[Double])], centroids: Array[Array[Double]], convThresh: Double): RDD[(Array[Double], Array[String])] = {
    // sort city names into clusters
    // take the points and create map - index of closest centroid => city name - these are key-value pairs
    val centroidToCityRDD = points.map { case (city, vector) =>
      val closestCentroidIndex = findClosestCentroid(vector, centroids)
      (closestCentroidIndex, city)
    }
    // group by centroid index to get all city names for each centroid
    val groupedByCentroid = centroidToCityRDD.groupByKey()
    // convert grouped RDD to map of centroids -> city names
    val centroidToCitiesRDD = groupedByCentroid.map { case(centroidIndex, cities) => (centroids(centroidIndex), cities.toArray)}

    // recompute centroids - mean of all points assigned to each cluster
    val newCentroids = recomputeCentroids(centroidToCitiesRDD, points)

    // check for convergence (if centroids don't change much)
    val converged = centroids.zip(newCentroids).forall {
      case (oldCentroid, newCentroid) =>
        euclideanDistance(oldCentroid, newCentroid) < convThresh
    }

    // Always calculate and print WCSS
    val wcss = calculateWCSS(centroidToCitiesRDD, points)
    println(s"WCSS for current run: $wcss")

    if (converged == true) {
      println("Converged!")
      return centroidToCitiesRDD
    }
    return kMeans(points, newCentroids, convThresh)
  }

  // for each centroid, compute new centroid by averaging the points assigned to it
  def recomputeCentroids(centroidsToCitiesRDD: RDD[(Array[Double], Array[String])], points: RDD[(String, Array[Double])]): Array[Array[Double]] = {
    val pointsMap = points.collectAsMap()
    val pointsBroadcast = points.context.broadcast(pointsMap)

    centroidsToCitiesRDD.map { case (centroid, cities) =>
      val pointsMap = pointsBroadcast.value

      // calculate mean
      val numCities = cities.length
      //      val featureLength = points.lookup(cities.head).headOption.getOrElse(Array()).length
      val zeroVector = Array.fill(pointsMap(cities.head).length)(0.0)
      val sum = cities.foldLeft(zeroVector) { (curSum, city) =>
        val cityVector = pointsMap.getOrElse(city, Array.fill(zeroVector.length)(0.0))
        //        val cityVector = points.lookup(city).headOption.getOrElse(Array())
        cityVector.zip(curSum).map { case (v1, v2) => v1 + v2}
      }
      sum.map(_ / numCities)
    }.collect()
  }

  def euclideanDistance(point: Array[Double], centroid: Array[Double]) : Double = {
    sqrt(point.zip(centroid).map { case (a, b) => (a - b) * (a-b)}.sum)
  }

  def findClosestCentroid(point: Array[Double], centroids: Array[Array[Double]]): Int = {
    val distances = centroids.map(centroid => euclideanDistance(point, centroid))
    distances.zipWithIndex.minBy(_._1)._2
  }
}
