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

    val trainObs = Reader.readFile(sqlContext, "/home/jiajing/Documents/data/train/")
    // trainObs.head(10).foreach(println)
    // println(priceObs.count())

    val trainRdd = TimeSeriesRDD.timeSeriesRDDByVarFromObservations(trainObs,
      "timestamp", "symbol", "price", "roc", "stod", "kdj", "macd")
    // trainRdd.cache()
    // println(trainRdd.count())
    // trainRdd.keys.foreach(println)

    val ga = new GeneticAlgorithm(100, 10, 100, 10, 10, 6, 0.3)
    val modelRdd = trainRdd.trainUniSeries(ga.fit)

    val testObs = Reader.readFile(sqlContext, "/home/jiajing/Documents/data/test/")
    val testRdd = TimeSeriesRDD.timeSeriesRDDByVarFromObservations(testObs,
      "timestamp", "symbol", "price", "roc", "stod", "kdj", "macd")

    val allRdd = trainRdd.join(testRdd).join(modelRdd)
    allRdd.mapValues(kv => kv._2.score(kv._1._1(0), kv._1._2(0))).foreach(println)

  }
}
