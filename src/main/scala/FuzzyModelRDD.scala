import org.apache.spark.{TaskContext, Partition}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by jiajing on 15-12-31.
  */
class FuzzyModelRDD[K](parent: RDD[(K, Array[FuzzyModel])])
                      (implicit val kClassTag: ClassTag[K])
  extends RDD[(K, Array[FuzzyModel])](parent){

  lazy val keys = parent.map(_._1).collect()

  def compute(split: Partition, context: TaskContext): Iterator[(K, Array[FuzzyModel])] = {
    parent.iterator(split, context)
  }

  protected def getPartitions: Array[Partition] = parent.partitions

}
