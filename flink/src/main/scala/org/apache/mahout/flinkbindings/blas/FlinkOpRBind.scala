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
package org.apache.mahout.flinkbindings.blas

import scala.reflect.ClassTag
import org.apache.mahout.math.drm.logical.OpRbind
import org.apache.mahout.flinkbindings.drm.FlinkDrm
import org.apache.mahout.flinkbindings.drm.RowsFlinkDrm
import org.apache.mahout.flinkbindings.drm.BlockifiedFlinkDrm
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.DataSet
import org.apache.mahout.math.Vector

object FlinkOpRBind {

  def rbind[K: ClassTag](op: OpRbind[K], A: FlinkDrm[K], B: FlinkDrm[K]): FlinkDrm[K] = {
    // note that indexes of B are already re-arranged prior to executing this code
    val res = A.deblockify.ds.union(B.deblockify.ds)
    new RowsFlinkDrm(res.asInstanceOf[DataSet[(K, Vector)]], ncol = op.ncol)
  }

}