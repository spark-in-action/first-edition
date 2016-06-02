
package org.sia.scala

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector}
import breeze.linalg.{DenseVector => BDV,SparseVector => BSV,Vector => BV}
import breeze.linalg.{DenseMatrix => BDM, Matrix => BM}
import breeze.linalg.{DenseMatrix => BDM}
import breeze.linalg.{DenseVector => BDV}
import breeze.linalg.{Matrix => BM}
import breeze.linalg.{SparseVector => BSV}
import breeze.linalg.{Vector => BV}
import breeze.linalg.{DenseMatrix => BDM}
import breeze.linalg.{DenseVector => BDV}
import breeze.linalg.{Matrix => BM}
import breeze.linalg.{SparseVector => BSV}
import breeze.linalg.{Vector => BV}

object BreezeMethods {
  //UTILITY METHODS FOR CONVERTING SPARK VECTORS AND MATRICES TO BREEZE
  def toBreezeV(v:Vector):BV[Double] = v match {
      case dv:DenseVector => new BDV(dv.values)
      case sv:SparseVector => new BSV(sv.indices, sv.values, sv.size)
  }
  
  import org.apache.spark.mllib.linalg.{DenseMatrix, SparseMatrix, Matrix, Matrices}
  import breeze.linalg.{DenseMatrix => BDM,CSCMatrix => BSM,Matrix => BM}
  def toBreezeM(m:Matrix):BM[Double] = m match {
      case dm:DenseMatrix => new BDM(dm.numRows, dm.numCols, dm.values)
      case sm:SparseMatrix => new BSM(sm.values, sm.numCols, sm.numRows, sm.colPtrs, sm.rowIndices)
  }
  
  import org.apache.spark.mllib.linalg.distributed.{RowMatrix, CoordinateMatrix, BlockMatrix, DistributedMatrix, MatrixEntry}
  def toBreezeD(dm:DistributedMatrix):BM[Double] = dm match {
      case rm:RowMatrix => {
        val m = rm.numRows().toInt
         val n = rm.numCols().toInt
         val mat = BDM.zeros[Double](m, n)
         var i = 0
         rm.rows.collect().foreach { vector =>
           for(j <- 0 to vector.size-1)
           {
             mat(i, j) = vector(j)
           }
           i += 1
         }
         mat
       }
      case cm:CoordinateMatrix => {
         val m = cm.numRows().toInt
         val n = cm.numCols().toInt
         val mat = BDM.zeros[Double](m, n)
         cm.entries.collect().foreach { case MatrixEntry(i, j, value) =>
           mat(i.toInt, j.toInt) = value
         }
         mat
      }
      case bm:BlockMatrix => {
         val localMat = bm.toLocalMatrix()
         new BDM[Double](localMat.numRows, localMat.numCols, localMat.toArray)
      }
  }
}

