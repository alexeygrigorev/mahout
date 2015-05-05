package org.apache.mahout.flinkbindings.blas

import org.scalatest.FunSuite
import org.apache.mahout.math._
import scalabindings._
import RLikeOps._
import drm._
import org.apache.mahout.flinkbindings._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.mahout.math.drm.logical.OpAx
import org.apache.mahout.flinkbindings.drm.CheckpointedFlinkDrm
import org.apache.mahout.flinkbindings.drm.RowsFlinkDrm
import org.apache.mahout.math.drm.logical.OpAt
import org.apache.mahout.math.drm.logical.OpAtB

@RunWith(classOf[JUnitRunner])
class LATestSuit extends FunSuite with DistributedFlinkSuit {

  test("Ax blockified") {
    val inCoreA = dense((1, 2, 3), (2, 3, 4), (3, 4, 5))
    val A = drmParallelize(m = inCoreA, numPartitions = 2)
    val x: Vector = (0, 1, 2)

    val opAx = new OpAx(A, x)
    val res = FlinkOpAx.blockifiedBroadcastAx(opAx, A)
    val drm = new CheckpointedFlinkDrm(res.deblockify.ds)
    val output = drm.collect

    val b = output(::, 0)
    assert(b == dvec(8, 11, 14))
  }

  test("At sparseTrick") {
    val inCoreA = dense((1, 2, 3), (2, 3, 4))
    val A = drmParallelize(m = inCoreA, numPartitions = 2)

    val opAt = new OpAt(A)
    val res = FlinkOpAt.sparseTrick(opAt, A)
    val drm = new CheckpointedFlinkDrm(res.deblockify.ds, _nrow=inCoreA.ncol, _ncol=inCoreA.nrow)
    val output = drm.collect

    assert((output - inCoreA.t).norm < 1e-6)
  }

  test("AtB notZippable") {
    val inCoreAt = dense((1, 2), (2, 3), (3, 4))

    val At = drmParallelize(m = inCoreAt, numPartitions = 2)

    val inCoreB = dense((1, 2), (3, 4), (11, 4))
    val B = drmParallelize(m = inCoreB, numPartitions = 2)

    val opAtB = new OpAtB(At, B)
    val res = FlinkOpAtB.notZippable(opAtB, At, B)

    val drm = new CheckpointedFlinkDrm(res.deblockify.ds, _nrow=inCoreAt.ncol, _ncol=inCoreB.ncol)
    val output = drm.collect

    val expected = inCoreAt.t %*% inCoreB
    assert((output - expected).norm < 1e-6)
  }
  

}