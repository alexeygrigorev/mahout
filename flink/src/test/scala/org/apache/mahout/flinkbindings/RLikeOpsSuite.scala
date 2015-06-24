/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.mahout.flinkbindings

import org.apache.mahout.flinkbindings._
import org.apache.mahout.math._
import org.apache.mahout.math.drm._
import org.apache.mahout.math.drm.RLikeDrmOps._
import org.apache.mahout.math.scalabindings._
import org.apache.mahout.math.scalabindings.RLikeOps._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.slf4j.Logger
import org.slf4j.LoggerFactory


@RunWith(classOf[JUnitRunner])
class RLikeOpsSuite extends FunSuite with DistributedFlinkSuit {

  val LOGGER = LoggerFactory.getLogger(getClass())

  test("A %*% x") {
    val inCoreA = dense((1, 2, 3), (2, 3, 4), (3, 4, 5))
    val A = drmParallelize(m = inCoreA, numPartitions = 2)
    val x: Vector = (0, 1, 2)

    val res = A %*% x

    val b = res.collect(::, 0)
    assert(b == dvec(8, 11, 14))
  }

  test("A.t") {
    val inCoreA = dense((1, 2, 3), (2, 3, 4))
    val A = drmParallelize(m = inCoreA, numPartitions = 2)
    val res = A.t.collect

    val expected = inCoreA.t
    assert((res - expected).norm < 1e-6)
  }

  test("A.t %*% x") {
    val inCoreA = dense((1, 2, 3), (2, 3, 4))
    val A = drmParallelize(m = inCoreA, numPartitions = 2)
    val x = dvec(3, 11)
    val res = (A.t %*% x).collect(::, 0)

    val expected = inCoreA.t %*% x 
    assert((res - expected).norm(2) < 1e-6)
  }

  test("A.t %*% B") {
    val inCoreA = dense((1, 2), (2, 3), (3, 4))
    val inCoreB = dense((1, 2), (3, 4), (11, 4))

    val A = drmParallelize(m = inCoreA, numPartitions = 2)
    val B = drmParallelize(m = inCoreB, numPartitions = 2)

    val res = A.t %*% B

    val expected = inCoreA.t %*% inCoreB
    assert((res.collect - expected).norm < 1e-6)
  }

  test("A %*% B.t") {
    val inCoreA = dense((1, 2), (2, 3), (3, 4))
    val inCoreB = dense((1, 2), (3, 4), (11, 4))

    val A = drmParallelize(m = inCoreA, numPartitions = 2)
    val B = drmParallelize(m = inCoreB, numPartitions = 2)

    val res = A %*% B.t

    val expected = inCoreA %*% inCoreB.t
    assert((res.collect - expected).norm < 1e-6)
  }

  test("A.t %*% A") {
    val inCoreA = dense((1, 2), (2, 3), (3, 4))
    val A = drmParallelize(m = inCoreA, numPartitions = 2)

    val res = A.t %*% A

    val expected = inCoreA.t %*% inCoreA
    assert((res.collect - expected).norm < 1e-6)
  }

  test("A %*% B") {
    val inCoreA = dense((1, 2), (2, 3), (3, 4)).t
    val inCoreB = dense((1, 2), (3, 4), (11, 4))

    val A = drmParallelize(m = inCoreA, numPartitions = 2)
    val B = drmParallelize(m = inCoreB, numPartitions = 2)

    val res = A %*% B

    val expected = inCoreA %*% inCoreB
    assert((res.collect - expected).norm < 1e-6)
  }

  test("A * scalar") {
    val inCoreA = dense((1, 2), (2, 3), (3, 4))
    val A = drmParallelize(m = inCoreA, numPartitions = 2)

    val res = A * 5
    assert((res.collect - inCoreA * 5).norm < 1e-6)
  }

  test("A / scalar") {
    val inCoreA = dense((1, 2), (2, 3), (3, 4)).t
    val A = drmParallelize(m = inCoreA, numPartitions = 2)

    val res = A / 5
    assert((res.collect - (inCoreA / 5)).norm < 1e-6)
  }

  test("A + scalar") {
    val inCoreA = dense((1, 2), (2, 3), (3, 4))
    val A = drmParallelize(m = inCoreA, numPartitions = 2)

    val res = A + 5
    assert((res.collect - (inCoreA + 5)).norm < 1e-6)
  }

  test("A - scalar") {
    val inCoreA = dense((1, 2), (2, 3), (3, 4))
    val A = drmParallelize(m = inCoreA, numPartitions = 2)

    val res = A - 5
    assert((res.collect - (inCoreA - 5)).norm < 1e-6)
  }

  test("A * B") {
    val inCoreA = dense((1, 2), (2, 3), (3, 4))
    val inCoreB = dense((1, 2), (3, 4), (11, 4))

    val A = drmParallelize(m = inCoreA, numPartitions = 2)
    val B = drmParallelize(m = inCoreB, numPartitions = 2)

    val res = A * B
    val expected = inCoreA * inCoreB
    assert((res.collect - expected).norm < 1e-6)
  }

  test("A / B") {
    val inCoreA = dense((1, 2), (2, 3), (3, 4))
    val inCoreB = dense((1, 2), (3, 4), (11, 4))

    val A = drmParallelize(m = inCoreA, numPartitions = 2)
    val B = drmParallelize(m = inCoreB, numPartitions = 2)

    val res = A / B
    val expected = inCoreA / inCoreB
    assert((res.collect - expected).norm < 1e-6)
  }

  test("A + B") {
    val inCoreA = dense((1, 2), (2, 3), (3, 4))
    val inCoreB = dense((1, 2), (3, 4), (11, 4))

    val A = drmParallelize(m = inCoreA, numPartitions = 2)
    val B = drmParallelize(m = inCoreB, numPartitions = 2)

    val res = A + B
    val expected = inCoreA + inCoreB
    assert((res.collect - expected).norm < 1e-6)
  }

  test("A - B") {
    val inCoreA = dense((1, 2), (2, 3), (3, 4))
    val inCoreB = dense((1, 2), (3, 4), (11, 4))

    val A = drmParallelize(m = inCoreA, numPartitions = 2)
    val B = drmParallelize(m = inCoreB, numPartitions = 2)

    val res = A - B
    val expected = inCoreA - inCoreB
    assert((res.collect - expected).norm < 1e-6)
  }

  test("A cbind B") {
    val inCoreA = dense((1, 2), (2, 3), (3, 4))
    val inCoreB = dense((1, 2), (3, 4), (11, 4))
    val A = drmParallelize(m = inCoreA, numPartitions = 2)
    val B = drmParallelize(m = inCoreB, numPartitions = 2)

    val res = A cbind B
    val expected = dense((1, 2, 1, 2), (2, 3, 3, 4), (3, 4, 11, 4))
    assert((res.collect - expected).norm < 1e-6)
  }

  test("A rbind B") {
    val inCoreA = dense((1, 2), (2, 3), (3, 4))
    val inCoreB = dense((1, 2), (3, 4), (11, 4))
    val A = drmParallelize(m = inCoreA, numPartitions = 2)
    val B = drmParallelize(m = inCoreB, numPartitions = 2)

    val res = A rbind B
    val expected = dense((1, 2), (2, 3), (3, 4), (1, 2), (3, 4), (11, 4))
    assert((res.collect - expected).norm < 1e-6)
  }

  test("A row slice") {
    val inCoreA = dense((1, 2), (2, 3), (3, 4), (4, 4), (5, 5), (6, 7))
    val A = drmParallelize(m = inCoreA, numPartitions = 2)

    val res = A(2 until 5, ::)
    val expected = inCoreA(2 until 5, ::)
    assert((res.collect - expected).norm < 1e-6)
  }

  test("A column slice") {
    val inCoreA = dense((1, 2, 1, 2), (2, 3, 3, 4), (3, 4, 11, 4))
    val A = drmParallelize(m = inCoreA, numPartitions = 2)

    val res = A(::, 0 until 2)
    val expected = inCoreA(::, 0 until 2)
    assert((res.collect - expected).norm < 1e-6)
  }

  test("A %*% inCoreB") {
    val inCoreA = dense((1, 2), (2, 3), (3, 4)).t
    val inCoreB = dense((1, 2), (3, 4), (11, 4))

    val A = drmParallelize(m = inCoreA, numPartitions = 2)

    val res = A %*% inCoreB

    val expected = inCoreA %*% inCoreB
    assert((res.collect - expected).norm < 1e-6)
  }

  test("drmBroadcast") {
    val inCoreA = dense((1, 2), (3, 4), (11, 4))
    val x = dvec(1, 2)
    val A = drmParallelize(m = inCoreA, numPartitions = 2)

    val b = drmBroadcast(x)

    val res = A.mapBlock(1) { case (idx, block) =>
      (idx, (block %*% b).toColMatrix)
    }

    val expected = inCoreA %*% x
    assert((res.collect(::, 0) - expected).norm(2) < 1e-6)
  }

}