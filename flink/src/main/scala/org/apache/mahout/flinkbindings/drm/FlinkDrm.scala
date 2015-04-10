package org.apache.mahout.flinkbindings.drm

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.mahout.flinkbindings.FlinkDistributedContext
import org.apache.mahout.math.Matrix
import org.apache.mahout.math.drm._
import org.apache.mahout.math.scalabindings._
import RLikeOps._
import org.apache.mahout.flinkbindings.DrmDataSet
import org.apache.mahout.flinkbindings.BlockifiedDrmDataSet
import org.apache.flink.api.common.functions.MapPartitionFunction
import org.apache.mahout.math.Vector
import java.lang.Iterable
import scala.collection.JavaConverters._
import org.apache.mahout.math.DenseMatrix
import scala.reflect.ClassTag
import org.apache.mahout.math.SparseRowMatrix

trait FlinkDrm[K] {
  def executionEnvironment: ExecutionEnvironment
  def context: FlinkDistributedContext
  def isBlockified: Boolean

  def blockify: BlockifiedFlinkDrm[K]
  def deblockify: RowsFlinkDrm[K]
}

class RowsFlinkDrm[K: ClassTag](val ds: DrmDataSet[K], val ncol: Int) extends FlinkDrm[K] {

  def executionEnvironment = ds.getExecutionEnvironment
  def context: FlinkDistributedContext = ds.getExecutionEnvironment

  def isBlockified = false

  def blockify(): BlockifiedFlinkDrm[K] = {

    val parts = ds.mapPartition(new MapPartitionFunction[DrmTuple[K], (Array[K], Matrix)] {
      def mapPartition(values: Iterable[DrmTuple[K]], out: Collector[(Array[K], Matrix)]): Unit = {
        val it = values.asScala.seq

        val (keys, vectors) = it.unzip
        val isDense = vectors.head.isDense

        if (isDense) {
          val matrix = new DenseMatrix(vectors.size, ncol)
          vectors.zipWithIndex.foreach { case (vec, idx) => matrix(idx, ::) := vec }
          out.collect((keys.toArray[K], matrix))
        } else {
          val matrix = new SparseRowMatrix(vectors.size, ncol, vectors.toArray)
          out.collect((keys.toArray[K], matrix))
        }
      }
    })

    new BlockifiedFlinkDrm(parts, ncol)
  }

  def deblockify = this

}

class BlockifiedFlinkDrm[K: ClassTag](val ds: BlockifiedDrmDataSet[K], val ncol: Int) extends FlinkDrm[K] {

  def executionEnvironment = ds.getExecutionEnvironment
  def context: FlinkDistributedContext = ds.getExecutionEnvironment

  def isBlockified = true

  def blockify = this

  def deblockify = {
    val out = ds.flatMap(new FlatMapFunction[(Array[K], Matrix), DrmTuple[K]] {
      def flatMap(typle: (Array[K], Matrix), out: Collector[DrmTuple[K]]): Unit = typle match {
        case (keys, block) => keys.view.zipWithIndex.foreach {
          case (key, idx) => {
            out.collect((key, block(idx, ::)))
          }
        }
      }
    })
    new RowsFlinkDrm(out, ncol)
  }
}