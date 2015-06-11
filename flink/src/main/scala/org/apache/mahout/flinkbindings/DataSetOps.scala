package org.apache.mahout.flinkbindings

import java.lang.Iterable
import java.util.Collections
import java.util.Comparator
import scala.collection.JavaConverters._
import org.apache.flink.util.Collector
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.common.functions.RichMapPartitionFunction
import org.apache.flink.configuration.Configuration
import scala.reflect.ClassTag


class DataSetOps[K: ClassTag](val ds: DataSet[K]) {

  /**
   * Implementation taken from http://stackoverflow.com/questions/30596556/zipwithindex-on-apache-flink
   * 
   * TODO: remove when FLINK-2152 is committed and released 
   */
  def zipWithIndex(): DataSet[(Long, K)] = {

    // first for each partition count the number of elements - to calculate the offsets
    val counts = ds.mapPartition(new RichMapPartitionFunction[K, (Int, Long)] {
      override def mapPartition(values: Iterable[K], out: Collector[(Int, Long)]): Unit = {
        val cnt: Long = values.asScala.count(_ => true)
        val subtaskIdx = getRuntimeContext.getIndexOfThisSubtask
        out.collect(subtaskIdx -> cnt)
      }
    })

    // then use the offsets to index items of each partition
    val zipped = ds.mapPartition(new RichMapPartitionFunction[K, (Long, K)] {
        var offset: Long = 0

        override def open(parameters: Configuration): Unit = {
          val offsetsJava: java.util.List[(Int, Long)] = 
                  getRuntimeContext.getBroadcastVariable("counts")
          val offsets = offsetsJava.asScala

          val sortedOffsets = 
            offsets sortBy { case (id, _) => id } map { case (_, cnt) => cnt }

          val subtaskId = getRuntimeContext.getIndexOfThisSubtask
          offset = sortedOffsets.take(subtaskId).sum
        }

        override def mapPartition(values: Iterable[K], out: Collector[(Long, K)]): Unit = {
          val it = values.asScala
          it.zipWithIndex.foreach { case (value, idx) =>
            out.collect((idx + offset, value))
          }
        }
    }).withBroadcastSet(counts, "counts");

    zipped
  }

}