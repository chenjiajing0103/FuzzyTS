import java.io.File
import java.sql.Timestamp
import java.time.LocalDate

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, DataFrame, SQLContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by jiajing on 15-12-30.
  */
object Reader {
  def readFolderList(path: String): Array[String] = {
    val folderList = new ArrayBuffer[String]()
    new File(path).listFiles().foreach( x =>
      if (x.isDirectory)
        folderList.append(x.getName)
    )
    folderList.toArray
  }

  def readFile(sqlContext: SQLContext, path: String): DataFrame = {
    val folders = Reader.readFolderList(path)
    folders.foreach(println)
    val allRowRdd = folders.map { folderName =>
      val rowRdd = sqlContext.sparkContext.textFile(path+"/"+folderName).map { line =>
        val tokens = line.split('\t')
        val times = tokens(0).split('/')
        val dt = LocalDate.of(times(0).toInt, times(1).toInt, times(2).toInt)
        val symbol = folderName + "/" + times(0)
        val price = tokens(1).toDouble
        val roc = tokens(2).toDouble
        val stod = tokens(3).toDouble
        val kdj = tokens(4).toDouble
        val macd = tokens(5).toDouble
        Row(Timestamp.valueOf(dt.atStartOfDay()), symbol, price,
          roc, stod, kdj, macd)
      }
      // println(rowRdd.first())
      // println(rowRdd.count())
      rowRdd
    }.reduce(_++_)
    val fields = Seq(
      StructField("timestamp", TimestampType, true),
      StructField("symbol", StringType, true),
      StructField("price", DoubleType, true),
      StructField("roc", DoubleType, true),
      StructField("stod", DoubleType, true),
      StructField("kdj", DoubleType, true),
      StructField("macd", DoubleType, true)
    )
    val schema = StructType(fields)
    sqlContext.createDataFrame(allRowRdd, schema)
  }
}
