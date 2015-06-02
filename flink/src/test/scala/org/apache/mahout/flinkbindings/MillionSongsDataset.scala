package org.apache.mahout.flinkbindings

import org.apache.flink.api.java.ExecutionEnvironment

import org.apache.mahout.math._
import org.apache.mahout.math.scalabindings._
import org.apache.mahout.math.drm._
import org.apache.mahout.flinkbindings._

import RLikeOps._
import RLikeDrmOps._

object MillionSongsDataset {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    implicit val ctx = new FlinkDistributedContext(env)

    val inCoreA = dense((1, 2, 3), (2, 3, 4), (3, 4, 5))
    val A = drmParallelize(m = inCoreA, numPartitions = 2)

    A.dfsWrite("file://c:/tmp/flink-dsl/")

  }

}

/**
16:14:46,338  INFO main TypeExtractor:analyzePojo:1346 - Class class scala.Tuple2 must have a default constructor to be used as a POJO.
 16:14:48,546  WARN main NativeCodeLoader:<clinit>:62 - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
 Exception in thread "main" org.apache.flink.api.common.functions.InvalidTypesException: The return type of function 'dfsWrite(CheckpointedFlinkDrm.scala:118)' could not be determined automatically, due to type erasure. You can give type information hints by using the returns(...) method on the result of the transformation call, or by letting your function implement the 'ResultTypeQueryable' interface.
    at org.apache.flink.api.java.DataSet.getType(DataSet.java:177)
    at org.apache.flink.api.java.DataSet.output(DataSet.java:1409)
    at org.apache.mahout.flinkbindings.drm.CheckpointedFlinkDrm.dfsWrite(CheckpointedFlinkDrm.scala:129)
    at org.apache.mahout.flinkbindings.MillionSongsDataset$.main(MillionSongsDataset.scala:22)
    at org.apache.mahout.flinkbindings.MillionSongsDataset.main(MillionSongsDataset.scala)
Caused by: org.apache.flink.api.common.functions.InvalidTypesException: The given class is no subclass of org.apache.hadoop.io.Writable
    at org.apache.flink.api.java.typeutils.WritableTypeInfo.getWritableTypeInfo(WritableTypeInfo.java:122)
    at org.apache.flink.api.java.typeutils.TypeExtractor.privateGetForClass(TypeExtractor.java:1157)
    at org.apache.flink.api.java.typeutils.TypeExtractor.privateGetForClass(TypeExtractor.java:1123)
    at org.apache.flink.api.java.typeutils.TypeExtractor.createTypeInfoWithTypeHierarchy(TypeExtractor.java:519)
    at org.apache.flink.api.java.typeutils.TypeExtractor.createTypeInfoWithTypeHierarchy(TypeExtractor.java:434)
    at org.apache.flink.api.java.typeutils.TypeExtractor.privateCreateTypeInfo(TypeExtractor.java:362)
    at org.apache.flink.api.java.typeutils.TypeExtractor.getUnaryOperatorReturnType(TypeExtractor.java:262)
    at org.apache.flink.api.java.typeutils.TypeExtractor.getMapReturnTypes(TypeExtractor.java:92)
    at org.apache.flink.api.java.DataSet.map(DataSet.java:214)
    at org.apache.mahout.flinkbindings.drm.CheckpointedFlinkDrm.dfsWrite(CheckpointedFlinkDrm.scala:118)
    ... 2 more
*/