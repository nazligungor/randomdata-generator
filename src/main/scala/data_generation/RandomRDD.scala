package data_generation

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

//fix the way columns are generated - tuple can have max 22 elements which limits the column number
//this can generate up to 2147483647
final class RandomPartition[A: ClassTag](val index: Int, numValues: Int, random: => A) extends Partition{
  def values: Iterator[A] = Iterator.fill(numValues)(random)
}

final class RandomRDD[A: ClassTag] (@transient private val sc: SparkContext, numSlices: Int, numValues: Int, random: => A) extends RDD[A](sc,deps=Seq.empty) {

  //if we had 10 slices and wanted 22 items then that would be 2 slices with 3 items and 8 slices with 2 items
  //Items in this case represents the # of rows

  private val valuesPerSlice = numValues / numSlices
  private val slicesWithExtraItem = numValues % numSlices

  override def compute(split: Partition, context: TaskContext): Iterator[A] = split.asInstanceOf[RandomPartition[A]].values

  override protected def getPartitions: Array[Partition] = ((0 until slicesWithExtraItem).view.map(new RandomPartition[A](_, valuesPerSlice + 1, random)) ++
    (slicesWithExtraItem until numSlices).view.map(new RandomPartition[A](_, valuesPerSlice, random))).toArray

}
