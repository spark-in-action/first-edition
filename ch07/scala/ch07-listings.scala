

//Section 7.2.1
import org.apache.spark.mllib.linalg.{Vectors,Vector}
val dv1:Vector = Vectors.dense(5.0,6.0,7.0,8.0)
val dv2:Vector = Vectors.dense(Array(5.0,6.0,7.0,8.0))
val sv:Vector = Vectors.sparse(4, Array(0,1,2,3), Array(5.0,6.0,7.0,8.0))
dv2(2)
dv1.size
dv2.toArray

//UTILITY METHODS FOR CONVERTING SPARK VECTORS AND MATRICES TO BREEZE
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector}
import breeze.linalg.{DenseVector => BDV,SparseVector => BSV,Vector => BV}
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

toBreezeV(dv1) + toBreezeV(dv2)
toBreezeV(dv1).dot(toBreezeV(dv2))

val dm:Matrix = Matrices.dense(2,3,Array(5.0,0.0,0.0,3.0,1.0,4.0))
val sm:Matrix = Matrices.sparse(2,3,Array(0,1,2,4),Array(0,1,0,1),Array(5.0,3.0,1.0,4.0))
sm.asInstanceOf[SparseMatrix].toDense
dm.asInstanceOf[DenseMatrix].toSparse
dm(1,1)
dm.transpose

//Section 7.2.2
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.apache.spark.mllib.linalg.distributed.IndexedRow
val rmind = new IndexedRowMatrix(rm.rows.zipWithIndex().map(x => IndexedRow(x._2, x._1)))

//Section 7.4
val housingLines = sc.textFile("first-edition/ch07/housing.data", 6)
val housingVals = housingLines.map(x => Vectors.dense(x.split(",").map(_.trim().toDouble)))

//Section 7.4.1
val housingMat = new RowMatrix(housingVals)
val housingStats = housingMat.computeColumnSummaryStatistics()
housingStats.min

//Section 7.4.2
val housingColSims = housingMat.columnSimilarities()

//UTILITY METHOD FOR PRETTY-PRINTING MATRICES
def printMat(mat:BM[Double]) = {
   print("            ")
   for(j <- 0 to mat.cols-1) print("%-10d".format(j));
   println
   for(i <- 0 to mat.rows-1) { print("%-6d".format(i)); for(j <- 0 to mat.cols-1) print(" %+9.3f".format(mat(i, j))); println }
}

printMat(toBreezeD(housingColSims))
/* SHOULD GIVE:
            0         1         2         3         4         5         6         7         8         9         10        11        12        13
0         +0,000    +0,004    +0,527    +0,052    +0,459    +0,363    +0,482    +0,169    +0,675    +0,563    +0,416    +0,288    +0,544    +0,224
1         +0,000    +0,000    +0,122    +0,078    +0,334    +0,467    +0,211    +0,673    +0,135    +0,297    +0,394    +0,464    +0,200    +0,528
2         +0,000    +0,000    +0,000    +0,256    +0,915    +0,824    +0,916    +0,565    +0,840    +0,931    +0,869    +0,779    +0,897    +0,693
3         +0,000    +0,000    +0,000    +0,000    +0,275    +0,271    +0,275    +0,184    +0,190    +0,230    +0,248    +0,266    +0,204    +0,307
4         +0,000    +0,000    +0,000    +0,000    +0,000    +0,966    +0,962    +0,780    +0,808    +0,957    +0,977    +0,929    +0,912    +0,873
5         +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,909    +0,880    +0,719    +0,906    +0,982    +0,966    +0,832    +0,949
6         +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,672    +0,801    +0,929    +0,930    +0,871    +0,918    +0,803
7         +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,485    +0,710    +0,856    +0,882    +0,644    +0,856
8         +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,917    +0,771    +0,642    +0,806    +0,588
9         +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,939    +0,854    +0,907    +0,789
10        +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,957    +0,887    +0,897
11        +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,799    +0,928
12        +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,670
13        +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,000    +0,000
*/
//Section 7.4.3
val housingCovar = housingMat.computeCovariance()
printMat(toBreezeM(housingCovar))
/* SHOULD GIVE:
            0         1         2         3         4         5         6         7         8         9         10        11        12        13
0        +73,987   -40,216   +23,992    -0,122    +0,420    -1,325   +85,405    -6,877   +46,848  +844,822    +5,399  -302,382   +27,986   -30,719
1        -40,216  +543,937   -85,413    -0,253    -1,396    +5,113  -373,902   +32,629   -63,349 -1236,454   -19,777  +373,721   -68,783   +77,315
2        +23,992   -85,413   +47,064    +0,110    +0,607    -1,888  +124,514   -10,228   +35,550  +833,360    +5,692  -223,580   +29,580   -30,521
3         -0,122    -0,253    +0,110    +0,065    +0,003    +0,016    +0,619    -0,053    -0,016    -1,523    -0,067    +1,131    -0,098    +0,409
4         +0,420    -1,396    +0,607    +0,003    +0,013    -0,025    +2,386    -0,188    +0,617   +13,046    +0,047    -4,021    +0,489    -0,455
5         -1,325    +5,113    -1,888    +0,016    -0,025    +0,494    -4,752    +0,304    -1,284   -34,583    -0,541    +8,215    -3,080    +4,493
6        +85,405  -373,902  +124,514    +0,619    +2,386    -4,752  +792,358   -44,329  +111,771 +2402,690   +15,937  -702,940  +121,078   -97,589
7         -6,877   +32,629   -10,228    -0,053    -0,188    +0,304   -44,329    +4,434    -9,068  -189,665    -1,060   +56,040    -7,473    +4,840
8        +46,848   -63,349   +35,550    -0,016    +0,617    -1,284  +111,771    -9,068   +75,816 +1335,757    +8,761  -353,276   +30,385   -30,561
9       +844,822 -1236,454  +833,360    -1,523   +13,046   -34,583 +2402,690  -189,665 +1335,757 +28404,759  +168,153 -6797,911  +654,715  -726,256
10        +5,399   -19,777    +5,692    -0,067    +0,047    -0,541   +15,937    -1,060    +8,761  +168,153    +4,687   -35,060    +5,783   -10,111
11      -302,382  +373,721  -223,580    +1,131    -4,021    +8,215  -702,940   +56,040  -353,276 -6797,911   -35,060 +8334,752  -238,668  +279,990
12       +27,986   -68,783   +29,580    -0,098    +0,489    -3,080  +121,078    -7,473   +30,385  +654,715    +5,783  -238,668   +50,995   -48,448
13       -30,719   +77,315   -30,521    +0,409    -0,455    +4,493   -97,589    +4,840   -30,561  -726,256   -10,111  +279,990   -48,448   +84,587
*/

//Section 7.4.4
import org.apache.spark.mllib.regression.LabeledPoint
val housingData = housingVals.map(x => { val a = x.toArray; LabeledPoint(a(a.length-1), Vectors.dense(a.slice(0, a.length-1))) })

//Section 7.4.5
val sets = housingData.randomSplit(Array(0.8, 0.2))
val housingTrain = sets(0)
val housingValid = sets(1)

//Section 7.4.6
import org.apache.spark.mllib.feature.StandardScaler
val scaler = new StandardScaler(true, true).fit(housingTrain.map(x => x.features))
val trainScaled = housingTrain.map(x => LabeledPoint(x.label, scaler.transform(x.features)))
val validScaled = housingValid.map(x => LabeledPoint(x.label, scaler.transform(x.features)))

//Section 7.5
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
val alg = new LinearRegressionWithSGD()
alg.setIntercept(true)
alg.optimizer.setNumIterations(200)
trainScaled.cache()
validScaled.cache()
val model = alg.run(trainScaled)

//Section 7.5.1
val validPredicts = validScaled.map(x => (model.predict(x.features), x.label))
validPredicts.collect()
val RMSE = math.sqrt(validPredicts.map{case(p,l) => math.pow(p-l,2)}.mean())

//Section 7.5.2
import org.apache.spark.mllib.evaluation.RegressionMetrics
val validMetrics = new RegressionMetrics(validPredicts)
validMetrics.rootMeanSquaredError
validMetrics.meanSquaredError

//Section 7.5.3
println(model.weights.toArray.map(x => x.abs).zipWithIndex.sortBy(_._1).mkString(", "))

//Section 7.5.4
model.save(sc, "hdfs:///path/to/saved/model")

import org.apache.spark.mllib.regression.LinearRegressionModel
val model = LinearRegressionModel.load(sc, "hdfs:///path/to/saved/model")


//Section 7.6.1
import org.apache.spark.rdd.RDD
def iterateLRwSGD(iterNums:Array[Int], stepSizes:Array[Double], train:RDD[LabeledPoint], test:RDD[LabeledPoint]) = {
  for(numIter <- iterNums; step <- stepSizes)
  {
    val alg = new LinearRegressionWithSGD()
    alg.setIntercept(true).optimizer.setNumIterations(numIter).setStepSize(step)
    val model = alg.run(train)
    val rescaledPredicts = train.map(x => (model.predict(x.features), x.label))
    val validPredicts = test.map(x => (model.predict(x.features), x.label))
    val meanSquared = math.sqrt(rescaledPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
    val meanSquaredValid = math.sqrt(validPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
    println("%d, %5.3f -> %.4f, %.4f".format(numIter, step, meanSquared, meanSquaredValid))
    //Uncomment if you wish to see weghts and intercept values:
    //println("%d, %4.2f -> %.4f, %.4f (%s, %f)".format(numIter, step, meanSquared, meanSquaredValid, model.weights, model.intercept))
  }
}
iterateLRwSGD(Array(200, 400, 600), Array(0.05, 0.1, 0.5, 1, 1.5, 2, 3), trainScaled, validScaled)
// Our results:
// 200, 0.050 -> 7.5420, 7.4786
// 200, 0.100 -> 5.0437, 5.0910
// 200, 0.500 -> 4.6920, 4.7814
// 200, 1.000 -> 4.6777, 4.7756
// 200, 1.500 -> 4.6751, 4.7761
// 200, 2.000 -> 4.6746, 4.7771
// 200, 3.000 -> 108738480856.3940, 122956877593.1419
// 400, 0.050 -> 5.8161, 5.8254
// 400, 0.100 -> 4.8069, 4.8689
// 400, 0.500 -> 4.6826, 4.7772
// 400, 1.000 -> 4.6753, 4.7760
// 400, 1.500 -> 4.6746, 4.7774
// 400, 2.000 -> 4.6745, 4.7780
// 400, 3.000 -> 25240554554.3096, 30621674955.1730
// 600, 0.050 -> 5.2510, 5.2877
// 600, 0.100 -> 4.7667, 4.8332
// 600, 0.500 -> 4.6792, 4.7759
// 600, 1.000 -> 4.6748, 4.7767
// 600, 1.500 -> 4.6745, 4.7779
// 600, 2.000 -> 4.6745, 4.7783
// 600, 3.000 -> 4977766834.6285, 6036973314.0450

//Section 7.6.2
def addHighPols(v:Vector): Vector =
{
  Vectors.dense(v.toArray.flatMap(x => Array(x, x*x)))
}
val housingHP = housingData.map(v => LabeledPoint(v.label, addHighPols(v.features)))

housingHP.first().features.size

val setsHP = housingHP.randomSplit(Array(0.8, 0.2))
val housingHPTrain = setsHP(0)
val housingHPValid = setsHP(1)
val scalerHP = new StandardScaler(true, true).fit(housingHPTrain.map(x => x.features))
val trainHPScaled = housingHPTrain.map(x => LabeledPoint(x.label, scalerHP.transform(x.features)))
val validHPScaled = housingHPValid.map(x => LabeledPoint(x.label, scalerHP.transform(x.features)))
trainHPScaled.cache()
validHPScaled.cache()

iterateLRwSGD(Array(200, 400), Array(0.4, 0.5, 0.6, 0.7, 0.9, 1.0, 1.1, 1.2, 1.3, 1.5), trainHPScaled, validHPScaled)
// Our results:
// 200, 0.400 -> 4.5423, 4.2002
// 200, 0.500 -> 4.4632, 4.1532
// 200, 0.600 -> 4.3946, 4.1150
// 200, 0.700 -> 4.3349, 4.0841
// 200, 0.900 -> 4.2366, 4.0392
// 200, 1.000 -> 4.1961, 4.0233
// 200, 1.100 -> 4.1605, 4.0108
// 200, 1.200 -> 4.1843, 4.0157
// 200, 1.300 -> 165.8268, 186.6295
// 200, 1.500 -> 182020974.1549, 186781045.5643
// 400, 0.400 -> 4.4117, 4.1243
// 400, 0.500 -> 4.3254, 4.0795
// 400, 0.600 -> 4.2540, 4.0466
// 400, 0.700 -> 4.1947, 4.0228
// 400, 0.900 -> 4.1032, 3.9947
// 400, 1.000 -> 4.0678, 3.9876
// 400, 1.100 -> 4.0378, 3.9836
// 400, 1.200 -> 4.0407, 3.9863
// 400, 1.300 -> 106.0047, 121.4576
// 400, 1.500 -> 162153976.4283, 163000519.6179

iterateLRwSGD(Array(200, 400, 800, 1000, 3000, 6000), Array(1.1), trainHPScaled, validHPScaled)
//Our results:
// 200, 1.100 -> 4.1605, 4.0108
// 400, 1.100 -> 4.0378, 3.9836
// 800, 1.100 -> 3.9438, 3.9901
// 1000, 1.100 -> 3.9199, 3.9982
// 3000, 1.100 -> 3.8332, 4.0633
// 6000, 1.100 -> 3.7915, 4.1138

//Section 7.6.3
iterateLRwSGD(Array(10000, 15000, 30000, 50000), Array(1.1), trainHPScaled, validHPScaled)
// Our results:
// 10000, 1.100 -> 3.7638, 4.1553
// 15000, 1.100 -> 3.7441, 4.1922
// 30000, 1.100 -> 3.7173, 4.2626
// 50000, 1.100 -> 3.7039, 4.3163

//Section 7.6.5
def iterateRidge(iterNums:Array[Int], stepSizes:Array[Double], regParam:Double, train:RDD[LabeledPoint], test:RDD[LabeledPoint]) = {
  import org.apache.spark.mllib.regression.RidgeRegressionWithSGD
  for(numIter <- iterNums; step <- stepSizes)
  {
    val alg = new RidgeRegressionWithSGD()
    alg.setIntercept(true)
    alg.optimizer.setNumIterations(numIter).setRegParam(regParam).setStepSize(step)
    val model = alg.run(train)
    val rescaledPredicts = train.map(x => (model.predict(x.features), x.label))
    val validPredicts = test.map(x => (model.predict(x.features), x.label))
    val meanSquared = math.sqrt(rescaledPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
    val meanSquaredValid = math.sqrt(validPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
    println("%d, %5.3f -> %.4f, %.4f".format(numIter, step, meanSquared, meanSquaredValid))
  }
}
def iterateLasso(iterNums:Array[Int], stepSizes:Array[Double], regParam:Double, train:RDD[LabeledPoint], test:RDD[LabeledPoint]) = {
  import org.apache.spark.mllib.regression.LassoWithSGD
  for(numIter <- iterNums; step <- stepSizes)
  {
    val alg = new LassoWithSGD()
    alg.setIntercept(true).optimizer.setNumIterations(numIter).setStepSize(step).setRegParam(regParam)
    val model = alg.run(train)
    val rescaledPredicts = train.map(x => (model.predict(x.features), x.label))
    val validPredicts = test.map(x => (model.predict(x.features), x.label))
    val meanSquared = math.sqrt(rescaledPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
    val meanSquaredValid = math.sqrt(validPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
    println("%d, %5.3f -> %.4f, %.4f".format(numIter, step, meanSquared, meanSquaredValid))
    println("\tweights: "+model.weights)
  }
}
iterateRidge(Array(200, 400, 1000, 3000, 6000, 10000), Array(1.1), 0.01, trainHPScaled, validHPScaled)
// Our results:
// 200, 1.100 -> 4.2354, 4.0095
// 400, 1.100 -> 4.1355, 3.9790
// 1000, 1.100 -> 4.0425, 3.9661
// 3000, 1.100 -> 3.9842, 3.9695
// 6000, 1.100 -> 3.9674, 3.9728
// 10000, 1.100 -> 3.9607, 3.9745
iterateLasso(Array(200, 400, 1000, 3000, 6000, 10000, 15000), Array(1.1), 0.01, trainHPScaled, validHPScaled)
//Our results:
// 200, 1.100 -> 4.1762, 4.0223
// 400, 1.100 -> 4.0632, 3.9964
// 1000, 1.100 -> 3.9496, 3.9987
// 3000, 1.100 -> 3.8636, 4.0362
// 6000, 1.100 -> 3.8239, 4.0705
// 10000, 1.100 -> 3.7985, 4.1014
// 15000, 1.100 -> 3.7806, 4.1304

//Section 7.7.1
def iterateLRwSGDBatch(iterNums:Array[Int], stepSizes:Array[Double], fractions:Array[Double], train:RDD[LabeledPoint], test:RDD[LabeledPoint]) = {
  for(numIter <- iterNums; step <- stepSizes; miniBFraction <- fractions)
  {
    val alg = new LinearRegressionWithSGD()
    alg.setIntercept(true).optimizer.setNumIterations(numIter).setStepSize(step)
    alg.optimizer.setMiniBatchFraction(miniBFraction)
    val model = alg.run(train)
    val rescaledPredicts = train.map(x => (model.predict(x.features), x.label))
    val validPredicts = test.map(x => (model.predict(x.features), x.label))
    val meanSquared = math.sqrt(rescaledPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
    val meanSquaredValid = math.sqrt(validPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
    println("%d, %5.3f %5.3f -> %.4f, %.4f".format(numIter, step, miniBFraction, meanSquared, meanSquaredValid))
  }
}
iterateLRwSGDBatch(Array(400, 1000), Array(0.05, 0.09, 0.1, 0.15, 0.2, 0.3, 0.35, 0.4, 0.5, 1), Array(0.01, 0.1), trainHPScaled, validHPScaled)
//Our results:
// 400, 0.050 0.010 -> 6.0134, 5.2776
// 400, 0.050 0.100 -> 5.8968, 4.9389
// 400, 0.090 0.010 -> 4.9918, 4.5734
// 400, 0.090 0.100 -> 4.9090, 4.3149
// 400, 0.100 0.010 -> 4.9461, 4.5755
// 400, 0.100 0.100 -> 4.8615, 4.3074
// 400, 0.150 0.010 -> 4.8683, 4.6778
// 400, 0.150 0.100 -> 4.7478, 4.2706
// 400, 0.200 0.010 -> 4.8797, 4.8388
// 400, 0.200 0.100 -> 4.6737, 4.2139
// 400, 0.300 0.010 -> 5.1358, 5.3565
// 400, 0.300 0.100 -> 4.5565, 4.1133
// 400, 0.350 0.010 -> 5.3556, 5.7160
// 400, 0.350 0.100 -> 4.5129, 4.0734
// 400, 0.400 0.010 -> 5.4826, 5.8964
// 400, 0.400 0.100 -> 4.5017, 4.0547
// 400, 0.500 0.010 -> 16.2895, 17.5951
// 400, 0.500 0.100 -> 5.0948, 4.6498
// 400, 1.000 0.010 -> 332757322790.5126, 346531473220.8046
// 400, 1.000 0.100 -> 179186352.6756, 189300221.1584
// 1000, 0.050 0.010 -> 5.0089, 4.5567
// 1000, 0.050 0.100 -> 4.9747, 4.3576
// 1000, 0.090 0.010 -> 4.7871, 4.5875
// 1000, 0.090 0.100 -> 4.7493, 4.3275
// 1000, 0.100 0.010 -> 4.7629, 4.5886
// 1000, 0.100 0.100 -> 4.7219, 4.3149
// 1000, 0.150 0.010 -> 4.6630, 4.5623
// 1000, 0.150 0.100 -> 4.6051, 4.2414
// 1000, 0.200 0.010 -> 4.5948, 4.5308
// 1000, 0.200 0.100 -> 4.5104, 4.1800
// 1000, 0.300 0.010 -> 4.5632, 4.5189
// 1000, 0.300 0.100 -> 4.3682, 4.0964
// 1000, 0.350 0.010 -> 4.5500, 4.5219
// 1000, 0.350 0.100 -> 4.3176, 4.0689
// 1000, 0.400 0.010 -> 4.4396, 4.3976
// 1000, 0.400 0.100 -> 4.2904, 4.0562
// 1000, 0.500 0.010 -> 11.6097, 12.0766
// 1000, 0.500 0.100 -> 4.5170, 4.3467
// 1000, 1.000 0.010 -> 67686532719.3362, 62690702177.4123
// 1000, 1.000 0.100 -> 103237131.4750, 119664651.1957
iterateLRwSGDBatch(Array(400, 1000, 2000, 3000, 5000, 10000), Array(0.4), Array(0.1, 0.2, 0.4, 0.5, 0.6, 0.8), trainHPScaled, validHPScaled)
//Our results:
// 400, 0.400 0.100 -> 4.5017, 4.0547
// 400, 0.400 0.200 -> 4.4509, 4.0288
// 400, 0.400 0.400 -> 4.4434, 4.1154
// 400, 0.400 0.500 -> 4.4059, 4.1588
// 400, 0.400 0.600 -> 4.4106, 4.1647
// 400, 0.400 0.800 -> 4.3930, 4.1245
// 1000, 0.400 0.100 -> 4.2904, 4.0562
// 1000, 0.400 0.200 -> 4.2470, 4.0363
// 1000, 0.400 0.400 -> 4.2490, 4.0181
// 1000, 0.400 0.500 -> 4.2201, 4.0372
// 1000, 0.400 0.600 -> 4.2209, 4.0357
// 1000, 0.400 0.800 -> 4.2139, 4.0173
// 2000, 0.400 0.100 -> 4.1367, 3.9843
// 2000, 0.400 0.200 -> 4.1030, 3.9847
// 2000, 0.400 0.400 -> 4.1129, 3.9736
// 2000, 0.400 0.500 -> 4.0934, 3.9652
// 2000, 0.400 0.600 -> 4.0926, 3.9849
// 2000, 0.400 0.800 -> 4.0893, 3.9793
// 3000, 0.400 0.100 -> 4.0677, 3.9342
// 3000, 0.400 0.200 -> 4.0366, 4.0256
// 3000, 0.400 0.400 -> 4.0408, 3.9815
// 3000, 0.400 0.500 -> 4.0282, 3.9833
// 3000, 0.400 0.600 -> 4.0271, 3.9863
// 3000, 0.400 0.800 -> 4.0253, 3.9754
// 5000, 0.400 0.100 -> 3.9826, 3.9597
// 5000, 0.400 0.200 -> 3.9653, 4.0212
// 5000, 0.400 0.400 -> 3.9654, 3.9801
// 5000, 0.400 0.500 -> 3.9600, 3.9780
// 5000, 0.400 0.600 -> 3.9591, 3.9774
// 5000, 0.400 0.800 -> 3.9585, 3.9761
// 10000, 0.400 0.100 -> 3.9020, 3.9701
// 10000, 0.400 0.200 -> 3.8927, 4.0307
// 10000, 0.400 0.400 -> 3.8920, 3.9958
// 10000, 0.400 0.500 -> 3.8900, 4.0092
// 10000, 0.400 0.600 -> 3.8895, 4.0061
// 10000, 0.400 0.800 -> 3.8895, 4.0199

//Section 7.7.2
import org.apache.log4j.Logger
import org.apache.log4j.Level
Logger.getLogger("breeze").setLevel(Level.WARN)
def iterateLBFGS(regParams:Array[Double], numCorrections:Int, tolerance:Double, train:RDD[LabeledPoint], test:RDD[LabeledPoint]) = {
  import org.apache.spark.mllib.optimization.LeastSquaresGradient
  import org.apache.spark.mllib.optimization.SquaredL2Updater
  import org.apache.spark.mllib.optimization.LBFGS
  import org.apache.spark.mllib.util.MLUtils
  val dimnum = train.first().features.size
  for(regParam <- regParams)
  {
    val (weights:Vector, loss:Array[Double]) = LBFGS.runLBFGS(
      train.map(x => (x.label, MLUtils.appendBias(x.features))),
      new LeastSquaresGradient(),
      new SquaredL2Updater(),
      numCorrections,
      tolerance,
      50000,
      regParam,
      Vectors.zeros(dimnum+1))

    val model = new LinearRegressionModel(
      Vectors.dense(weights.toArray.slice(0, weights.size - 1)),
      weights(weights.size - 1))

    val trainPredicts = train.map(x => (model.predict(x.features), x.label))
    val validPredicts = test.map(x => (model.predict(x.features), x.label))
    val meanSquared = math.sqrt(trainPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
    val meanSquaredValid = math.sqrt(validPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
    println("%5.3f, %d -> %.4f, %.4f".format(regParam, numCorrections, meanSquared, meanSquaredValid))
  }
}
iterateLBFGS(Array(0.005, 0.007, 0.01, 0.02, 0.03, 0.05, 0.1), 10, 1e-5, trainHPScaled, validHPScaled)
//Our results:
// 0.005, 10 -> 3.8335, 4.0383
// 0.007, 10 -> 3.8848, 4.0005
// 0.010, 10 -> 3.9542, 3.9798
// 0.020, 10 -> 4.1388, 3.9662
// 0.030, 10 -> 4.2892, 3.9996
// 0.050, 10 -> 4.5319, 4.0796
// 0.100, 10 -> 5.0571, 4.3579





