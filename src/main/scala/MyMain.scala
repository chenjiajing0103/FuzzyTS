import java.io.{File, PrintWriter}

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by jiajing on 15-12-30.
  */
object MyMain {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("fuzzy ts")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val trainObs = Reader.readFile(sqlContext, "/home/jiajing/Documents/data/train3/")
    // trainObs.head(10).foreach(println)
    // println(priceObs.count())

    val trainRdd = TimeSeriesRDD.timeSeriesRDDByVarFromObservations(trainObs,
      "timestamp", "symbol", "price", "roc", "stod", "kdj", "macd")
    // trainRdd.cache()
    // println(trainRdd.count())
    // trainRdd.keys.foreach(println)

    //val ga = new GeneticAlgorithm(1, 100, 100, 10, 10, 10, 6, 1, 0.3)
    val ga = new GeneticAlgorithm(1, 100, 200, 10, 10, 10, 6, 0.8, 0.01)
    val modelRdd = trainRdd.trainMultiSeries(ga.fit)
    //val modelRdd = trainRdd.trainUniSeries(ga.fit)

    val testObs = Reader.readFile(sqlContext, "/home/jiajing/Documents/data/test3/")
    val testRdd = TimeSeriesRDD.timeSeriesRDDByVarFromObservations(testObs,
      "timestamp", "symbol", "price", "roc", "stod", "kdj", "macd")

    //val writer = new PrintWriter(new File("/home/jiajing/Documents/result/v3.0.txt"))

    val allRdd = trainRdd.join(testRdd).join(modelRdd)
    //val allRdd = trainRdd.cogroup(testRdd).cogroup(modelRdd)
    val allResults = allRdd.mapValues{ kv =>
      val results = kv._2.map(_.score(kv._1._1, kv._1._2))
      //val results = kv._2.head.map(_.score(kv._1.head._1.head(0), kv._1.head._2.head(0)))
      results.sum / results.length
    }.sortByKey()
    allResults.collect.foreach(println)//foreach(writer.println)
    val tailResultsColl = allResults.values.collect.tail.take(9)
    println(tailResultsColl.sum / tailResultsColl.length)
    //writer.close()
    println("Done!")
  }
}
