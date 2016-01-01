import java.sql.Timestamp

import org.apache.spark.{TaskContext, Partition, HashPartitioner, Partitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * Created by jiajing on 15-12-30.
  */
class TimeSeriesRDD[K](parent: RDD[(K, Vector[Vector[Double]])])
                      (implicit val kClassTag: ClassTag[K])
  extends RDD[(K, Vector[Vector[Double]])](parent){

  lazy val keys = parent.map(_._1).collect()

  def compute(split: Partition, context: TaskContext): Iterator[(K, Vector[Vector[Double]])] = {
    parent.iterator(split, context)
  }

  protected def getPartitions: Array[Partition] = parent.partitions

  def findSeries(key: K): Vector[Vector[Double]] = {
    filter(_._1 == key).first()._2
  }

  override def filter(f: ((K, Vector[Vector[Double]])) => Boolean): TimeSeriesRDD[K] = {
    new TimeSeriesRDD[K](super.filter(f))
  }

  /*def mapSeries[U](f: (Vector) => Vector): TimeSeriesRDD[K] = {
    new TimeSeriesRDD[K](map(kt => (kt._1, f(kt._2))))
  }

  def seriesStats(): RDD[StatCounter] = {
    map(kt => new StatCounter(kt._2.valuesIterator))
  }*/

  // trans Vector[Vector] from row:time col:var to row:var col:time before doing this
  def trainUniSeries(f: (Vector[Double]) => FuzzyModel): FuzzyModelRDD[K] = {
    new FuzzyModelRDD[K](map(kt => (kt._1, f(kt._2(0)))))
  }

  //def trainMultiSeries(f: (Vector[Vector[Double]]) => FuzzyModel): FuzzyModelRDD[K] = {
  //  new FuzzyModelRDD[K](map(kt => (kt._1, f(kt._2))))
  //}

}

object TimeSeriesRDD {
  def timeSeriesRDDByTimeFromObservations(
      df: DataFrame,
      timeStampCol: String,
      keyCol: String,
      valueCol: String,
      rodCol: String,
      stodCol: String,
      kdjCol: String,
      macdCol: String): TimeSeriesRDD[String] = {
    val rdd = df.select(timeStampCol, keyCol, valueCol,
      rodCol, stodCol, kdjCol, macdCol).rdd.map { row =>
      ((row.getString(1), row.getAs[Timestamp](0)), Vector(row.getDouble(2),
        row.getDouble(3), row.getDouble(4), row.getDouble(5), row.getDouble(6)))
    }
    // sorted by keyCol first and then timestamp
    implicit val ordering = new Ordering[(String, Timestamp)] {
      override def compare(a: (String, Timestamp), b: (String, Timestamp)): Int = {
        val strCompare = a._1.compareTo(b._1)
        if (strCompare != 0) strCompare else a._2.compareTo(b._2)
      }
    }

    // do partition by tuple2._1 keyCol(symbol)
    val shuffled = rdd.repartitionAndSortWithinPartitions(new Partitioner() {
      val hashPartitioner = new HashPartitioner(rdd.partitions.size)
      override def numPartitions: Int = hashPartitioner.numPartitions
      override def getPartition(key: Any): Int =
        hashPartitioner.getPartition(key.asInstanceOf[(Any, Any)]._1)
    })

    new TimeSeriesRDD[String](shuffled.mapPartitions { iter =>
      val bufferedIter = iter.buffered
      new Iterator[(String, Vector[Vector[Double]])]() {
        override def hasNext: Boolean = bufferedIter.hasNext
        override def next(): (String, Vector[Vector[Double]]) = {
          val series = new ArrayBuffer[Vector[Double]]
          val first = bufferedIter.next()
          series.append(first._2)

          // splited by different keyCol
          val key = first._1._1
          while (bufferedIter.hasNext && bufferedIter.head._1._1 == key) {
            series.append(bufferedIter.next()._2)
          }
          (key, series.toVector)
        }
      }
    })
  }

  def timeSeriesRDDByVarFromObservations(
      df: DataFrame,
      timeStampCol: String,
      keyCol: String,
      valueCol: String,
      rodCol: String,
      stodCol: String,
      kdjCol: String,
      macdCol: String): TimeSeriesRDD[String] = {
    val rdd = df.select(timeStampCol, keyCol, valueCol,
      rodCol, stodCol, kdjCol, macdCol).rdd.map { row =>
      ((row.getString(1), row.getAs[Timestamp](0)), Vector(row.getDouble(2),
        row.getDouble(3), row.getDouble(4), row.getDouble(5), row.getDouble(6)))
    }
    // sorted by keyCol first and then timestamp
    implicit val ordering = new Ordering[(String, Timestamp)] {
      override def compare(a: (String, Timestamp), b: (String, Timestamp)): Int = {
        val strCompare = a._1.compareTo(b._1)
        if (strCompare != 0) strCompare else a._2.compareTo(b._2)
      }
    }

    // do partition by tuple2._1 keyCol(symbol)
    val shuffled = rdd.repartitionAndSortWithinPartitions(new Partitioner() {
      val hashPartitioner = new HashPartitioner(rdd.partitions.size)
      override def numPartitions: Int = hashPartitioner.numPartitions
      override def getPartition(key: Any): Int =
        hashPartitioner.getPartition(key.asInstanceOf[(Any, Any)]._1)
    })

    new TimeSeriesRDD[String](shuffled.mapPartitions { iter =>
      val bufferedIter = iter.buffered
      new Iterator[(String, Vector[Vector[Double]])]() {
        override def hasNext: Boolean = bufferedIter.hasNext
        override def next(): (String, Vector[Vector[Double]]) = {
          val valueSeries = new ArrayBuffer[Double]()
          val rocSeries = new ArrayBuffer[Double]()
          val stodSeries = new ArrayBuffer[Double]()
          val kdjSeries = new ArrayBuffer[Double]()
          val macdSeries = new ArrayBuffer[Double]()

          val first = bufferedIter.next()
          valueSeries.append(first._2(0))
          rocSeries.append(first._2(1))
          stodSeries.append(first._2(2))
          kdjSeries.append(first._2(3))
          macdSeries.append(first._2(4))

          // splited by different keyCol
          val key = first._1._1
          while (bufferedIter.hasNext && bufferedIter.head._1._1 == key) {
            val nextElem = bufferedIter.next()
            valueSeries.append(nextElem._2(0))
            rocSeries.append(nextElem._2(1))
            stodSeries.append(nextElem._2(2))
            kdjSeries.append(nextElem._2(3))
            macdSeries.append(nextElem._2(4))
          }
          val series = Vector(valueSeries.toVector, rocSeries.toVector,
            stodSeries.toVector, kdjSeries.toVector, macdSeries.toVector)
          (key, series)
        }
      }
    })
  }
}
