from __future__ import print_function

#Section 7.2.1
from pyspark.mllib.linalg import Vectors, Vector
dv1 = Vectors.dense(5.0,6.0,7.0,8.0)
dv2 = Vectors.dense([5.0,6.0,7.0,8.0])
sv = Vectors.sparse(4, [0,1,2,3], [5.0,6.0,7.0,8.0])
dv2[2]
dv1.size
dv2.toArray()

from pyspark.mllib.linalg import Matrices

dm = Matrices.dense(2,3,[5.0,0.0,0.0,3.0,1.0,4.0])
sm = Matrices.sparse(2,3,[0,1,2,4], [0,1,0,1], [5.0,3.0,1.0,4.0])
sm.toDense()
dm.toSparse()
dm[1,1]

#Section 7.2.2
from pyspark.mllib.linalg.distributed import IndexedRowMatrix, IndexedRow
rmind = IndexedRowMatrix(rm.rows().zipWithIndex().map(lambda x: IndexedRow(x[1], x[0])))

#Section 7.4
housingLines = sc.textFile("first-edition/ch07/housing.data", 6)
housingVals = housingLines.map(lambda x: Vectors.dense([float(v.strip()) for v in x.split(",")]))

#Section 7.4.1
from pyspark.mllib.linalg.distributed import RowMatrix
housingMat = RowMatrix(housingVals)
from pyspark.mllib.stat._statistics import Statistics
housingStats = Statistics.colStats(housingVals)
housingStats.min()

#Section 7.4.4
from pyspark.mllib.regression import LabeledPoint
def toLabeledPoint(x):
  a = x.toArray()
  return LabeledPoint(a[-1], Vectors.dense(a[0:-1]))

housingData = housingVals.map(toLabeledPoint)

#Section 7.4.5
sets = housingData.randomSplit([0.8, 0.2])
housingTrain = sets[0]
housingValid = sets[1]

#Section 7.4.6
from pyspark.mllib.feature import StandardScaler
scaler = StandardScaler(True, True).fit(housingTrain.map(lambda x: x.features))
trainLabel = housingTrain.map(lambda x: x.label)
trainFeatures = housingTrain.map(lambda x: x.features)
validLabel = housingValid.map(lambda x: x.label)
validFeatures = housingValid.map(lambda x: x.features)
trainScaled = trainLabel.zip(scaler.transform(trainFeatures)).map(lambda x: LabeledPoint(x[0], x[1]))
validScaled = validLabel.zip(scaler.transform(validFeatures)).map(lambda x: LabeledPoint(x[0], x[1]))

#Section 7.5
from pyspark.mllib.regression import LinearRegressionWithSGD
alg = LinearRegressionWithSGD()
trainScaled.cache()
validScaled.cache()
model = alg.train(trainScaled, iterations=200, intercept=True)

#Section 7.5.1
validPredicts = validScaled.map(lambda x: (float(model.predict(x.features)), x.label))
validPredicts.collect()
import math
RMSE = math.sqrt(validPredicts.map(lambda p: pow(p[0]-p[1],2)).mean())

#Section 7.5.2
from pyspark.mllib.evaluation import RegressionMetrics
validMetrics = RegressionMetrics(validPredicts)
validMetrics.rootMeanSquaredError
validMetrics.meanSquaredError

#Section 7.5.3
import operator
print(",".join([str(s) for s in sorted(enumerate([abs(x) for x in model.weights.toArray()]), key=operator.itemgetter(0))]))

#Section 7.5.4
model.save(sc, "ch07output/model")

from pyspark.mllib.regression import LinearRegressionModel
model = LinearRegressionModel.load(sc, "ch07output/model")


#Section 7.6.1
def iterateLRwSGD(iterNums, stepSizes, train, valid):
  from pyspark.mllib.regression import LinearRegressionWithSGD
  import math
  for numIter in iterNums:
    for step in stepSizes:
      alg = LinearRegressionWithSGD()
      model = alg.train(train, iterations=numIter, step=step, intercept=True)
      rescaledPredicts = train.map(lambda x: (float(model.predict(x.features)), x.label))
      validPredicts = valid.map(lambda x: (float(model.predict(x.features)), x.label))
      meanSquared = math.sqrt(rescaledPredicts.map(lambda p: pow(p[0]-p[1],2)).mean())
      meanSquaredValid = math.sqrt(validPredicts.map(lambda p: pow(p[0]-p[1],2)).mean())
      print("%d, %5.3f -> %.4f, %.4f" % (numIter, step, meanSquared, meanSquaredValid))
      #Uncomment if you wish to see weghts and intercept values:
      #print("%d, %4.2f -> %.4f, %.4f (%s, %f)" % (numIter, step, meanSquared, meanSquaredValid, model.weights, model.intercept))

iterateLRwSGD([200, 400, 600], [0.05, 0.1, 0.5, 1, 1.5, 2, 3], trainScaled, validScaled)
# Our results:
# 200, 0.050 -> 7.5420, 7.4786
# 200, 0.100 -> 5.0437, 5.0910
# 200, 0.500 -> 4.6920, 4.7814
# 200, 1.000 -> 4.6777, 4.7756
# 200, 1.500 -> 4.6751, 4.7761
# 200, 2.000 -> 4.6746, 4.7771
# 200, 3.000 -> 108738480856.3940, 122956877593.1419
# 400, 0.050 -> 5.8161, 5.8254
# 400, 0.100 -> 4.8069, 4.8689
# 400, 0.500 -> 4.6826, 4.7772
# 400, 1.000 -> 4.6753, 4.7760
# 400, 1.500 -> 4.6746, 4.7774
# 400, 2.000 -> 4.6745, 4.7780
# 400, 3.000 -> 25240554554.3096, 30621674955.1730
# 600, 0.050 -> 5.2510, 5.2877
# 600, 0.100 -> 4.7667, 4.8332
# 600, 0.500 -> 4.6792, 4.7759
# 600, 1.000 -> 4.6748, 4.7767
# 600, 1.500 -> 4.6745, 4.7779
# 600, 2.000 -> 4.6745, 4.7783
# 600, 3.000 -> 4977766834.6285, 6036973314.0450

#Section 7.6.2

def addHighPols(v):
  import itertools
  a = [[x, x*x] for x in v.toArray()]
  return Vectors.dense(list(itertools.chain(*a)))

housingHP = housingData.map(lambda v: LabeledPoint(v.label, addHighPols(v.features)))

len(housingHP.first().features)

setsHP = housingHP.randomSplit([0.8, 0.2])
housingHPTrain = setsHP[0]
housingHPValid = setsHP[1]
scalerHP = StandardScaler(True, True).fit(housingHPTrain.map(lambda x: x.features))
trainHPLabel = housingHPTrain.map(lambda x: x.label)
trainHPFeatures = housingHPTrain.map(lambda x: x.features)
validHPLabel = housingHPValid.map(lambda x: x.label)
validHPFeatures = housingHPValid.map(lambda x: x.features)
trainHPScaled = trainHPLabel.zip(scalerHP.transform(trainHPFeatures)).map(lambda x: LabeledPoint(x[0], x[1]))
validHPScaled = validHPLabel.zip(scalerHP.transform(validHPFeatures)).map(lambda x: LabeledPoint(x[0], x[1]))
trainHPScaled.cache()
validHPScaled.cache()

iterateLRwSGD([200, 400], [0.4, 0.5, 0.6, 0.7, 0.9, 1.0, 1.1, 1.2, 1.3, 1.5], trainHPScaled, validHPScaled)
# Our results:
# 200, 0.400 -> 4.5423, 4.2002
# 200, 0.500 -> 4.4632, 4.1532
# 200, 0.600 -> 4.3946, 4.1150
# 200, 0.700 -> 4.3349, 4.0841
# 200, 0.900 -> 4.2366, 4.0392
# 200, 1.000 -> 4.1961, 4.0233
# 200, 1.100 -> 4.1605, 4.0108
# 200, 1.200 -> 4.1843, 4.0157
# 200, 1.300 -> 165.8268, 186.6295
# 200, 1.500 -> 182020974.1549, 186781045.5643
# 400, 0.400 -> 4.4117, 4.1243
# 400, 0.500 -> 4.3254, 4.0795
# 400, 0.600 -> 4.2540, 4.0466
# 400, 0.700 -> 4.1947, 4.0228
# 400, 0.900 -> 4.1032, 3.9947
# 400, 1.000 -> 4.0678, 3.9876
# 400, 1.100 -> 4.0378, 3.9836
# 400, 1.200 -> 4.0407, 3.9863
# 400, 1.300 -> 106.0047, 121.4576
# 400, 1.500 -> 162153976.4283, 163000519.6179

iterateLRwSGD([200, 400, 800, 1000, 3000, 6000], [1.1], trainHPScaled, validHPScaled)
#Our results:
# 200, 1.100 -> 4.1605, 4.0108
# 400, 1.100 -> 4.0378, 3.9836
# 800, 1.100 -> 3.9438, 3.9901
# 1000, 1.100 -> 3.9199, 3.9982
# 3000, 1.100 -> 3.8332, 4.0633
# 6000, 1.100 -> 3.7915, 4.1138

#Section 7.6.3
iterateLRwSGD([10000, 15000, 30000, 50000], [1.1], trainHPScaled, validHPScaled)
# Our results:
# 10000, 1.100 -> 3.7638, 4.1553
# 15000, 1.100 -> 3.7441, 4.1922
# 30000, 1.100 -> 3.7173, 4.2626
# 50000, 1.100 -> 3.7039, 4.3163

#Section 7.6.5
def iterateRidge(iterNums, stepSizes, regParam, train, valid):
  from pyspark.mllib.regression import RidgeRegressionWithSGD
  import math
  for numIter in iterNums:
    for step in stepSizes:
      alg = RidgeRegressionWithSGD()
      model = alg.train(train, intercept=True, regParam=regParam, iterations=numIter, step=step)
      rescaledPredicts = train.map(lambda x: (model.predict(x.features), x.label))
      validPredicts = valid.map(lambda x: (model.predict(x.features), x.label))
      meanSquared = math.sqrt(rescaledPredicts.map(lambda p: pow(p[0]-p[1],2)).mean())
      meanSquaredValid = math.sqrt(validPredicts.map(lambda p: pow(p[0]-p[1],2)).mean())
      print("%d, %5.3f -> %.4f, %.4f" % (numIter, step, meanSquared, meanSquaredValid))

def iterateLasso(iterNums, stepSizes, regParam, train, valid):
  from pyspark.mllib.regression import LassoWithSGD
  for numIter in iterNums:
    for step in stepSizes:
      alg = LassoWithSGD()
      model = alg.train(train, intercept=True, iterations=numIter, step=step, regParam=regParam)
      rescaledPredicts = train.map(lambda x: (model.predict(x.features), x.label))
      validPredicts = valid.map(lambda x: (model.predict(x.features), x.label))
      meanSquared = math.sqrt(rescaledPredicts.map(lambda p: pow(p[0]-p[1],2)).mean())
      meanSquaredValid = math.sqrt(validPredicts.map(lambda p: pow(p[0]-p[1],2)).mean())
      print("%d, %5.3f -> %.4f, %.4f" % (numIter, step, meanSquared, meanSquaredValid))
      #print("\tweights: %s" % model.weights)

iterateRidge([200, 400, 1000, 3000, 6000, 10000], [1.1], 0.01, trainHPScaled, validHPScaled)
# Our results:
# 200, 1.100 -> 4.2354, 4.0095
# 400, 1.100 -> 4.1355, 3.9790
# 1000, 1.100 -> 4.0425, 3.9661
# 3000, 1.100 -> 3.9842, 3.9695
# 6000, 1.100 -> 3.9674, 3.9728
# 10000, 1.100 -> 3.9607, 3.9745
iterateLasso([200, 400, 1000, 3000, 6000, 10000, 15000], [1.1], 0.01, trainHPScaled, validHPScaled)
#Our results:
# 200, 1.100 -> 4.1762, 4.0223
# 400, 1.100 -> 4.0632, 3.9964
# 1000, 1.100 -> 3.9496, 3.9987
# 3000, 1.100 -> 3.8636, 4.0362
# 6000, 1.100 -> 3.8239, 4.0705
# 10000, 1.100 -> 3.7985, 4.1014
# 15000, 1.100 -> 3.7806, 4.1304

#Section 7.7.1
def iterateLRwSGDBatch(iterNums, stepSizes, fractions, train, valid):
  for numIter in iterNums:
    for step in stepSizes:
      for miniBFraction in fractions:
        alg = LinearRegressionWithSGD()
        model = alg.train(train, intercept=True, iterations=numIter, step=step, miniBatchFraction=miniBFraction)
        rescaledPredicts = train.map(lambda x: (model.predict(x.features), x.label))
        validPredicts = valid.map(lambda x: (model.predict(x.features), x.label))
        meanSquared = math.sqrt(rescaledPredicts.map(lambda p: pow(p[0]-p[1],2)).mean())
        meanSquaredValid = math.sqrt(validPredicts.map(lambda p: pow(p[0]-p[1],2)).mean())
        print("%d, %5.3f %5.3f -> %.4f, %.4f" % (numIter, step, miniBFraction, meanSquared, meanSquaredValid))

iterateLRwSGDBatch([400, 1000], [0.05, 0.09, 0.1, 0.15, 0.2, 0.3, 0.35, 0.4, 0.5, 1], [0.01, 0.1], trainHPScaled, validHPScaled)
#Our results:
# 400, 0.050 0.010 -> 6.0134, 5.2776
# 400, 0.050 0.100 -> 5.8968, 4.9389
# 400, 0.090 0.010 -> 4.9918, 4.5734
# 400, 0.090 0.100 -> 4.9090, 4.3149
# 400, 0.100 0.010 -> 4.9461, 4.5755
# 400, 0.100 0.100 -> 4.8615, 4.3074
# 400, 0.150 0.010 -> 4.8683, 4.6778
# 400, 0.150 0.100 -> 4.7478, 4.2706
# 400, 0.200 0.010 -> 4.8797, 4.8388
# 400, 0.200 0.100 -> 4.6737, 4.2139
# 400, 0.300 0.010 -> 5.1358, 5.3565
# 400, 0.300 0.100 -> 4.5565, 4.1133
# 400, 0.350 0.010 -> 5.3556, 5.7160
# 400, 0.350 0.100 -> 4.5129, 4.0734
# 400, 0.400 0.010 -> 5.4826, 5.8964
# 400, 0.400 0.100 -> 4.5017, 4.0547
# 400, 0.500 0.010 -> 16.2895, 17.5951
# 400, 0.500 0.100 -> 5.0948, 4.6498
# 400, 1.000 0.010 -> 332757322790.5126, 346531473220.8046
# 400, 1.000 0.100 -> 179186352.6756, 189300221.1584
# 1000, 0.050 0.010 -> 5.0089, 4.5567
# 1000, 0.050 0.100 -> 4.9747, 4.3576
# 1000, 0.090 0.010 -> 4.7871, 4.5875
# 1000, 0.090 0.100 -> 4.7493, 4.3275
# 1000, 0.100 0.010 -> 4.7629, 4.5886
# 1000, 0.100 0.100 -> 4.7219, 4.3149
# 1000, 0.150 0.010 -> 4.6630, 4.5623
# 1000, 0.150 0.100 -> 4.6051, 4.2414
# 1000, 0.200 0.010 -> 4.5948, 4.5308
# 1000, 0.200 0.100 -> 4.5104, 4.1800
# 1000, 0.300 0.010 -> 4.5632, 4.5189
# 1000, 0.300 0.100 -> 4.3682, 4.0964
# 1000, 0.350 0.010 -> 4.5500, 4.5219
# 1000, 0.350 0.100 -> 4.3176, 4.0689
# 1000, 0.400 0.010 -> 4.4396, 4.3976
# 1000, 0.400 0.100 -> 4.2904, 4.0562
# 1000, 0.500 0.010 -> 11.6097, 12.0766
# 1000, 0.500 0.100 -> 4.5170, 4.3467
# 1000, 1.000 0.010 -> 67686532719.3362, 62690702177.4123
# 1000, 1.000 0.100 -> 103237131.4750, 119664651.1957
iterateLRwSGDBatch([400, 1000, 2000, 3000, 5000, 10000], [0.4], [0.1, 0.2, 0.4, 0.5, 0.6, 0.8], trainHPScaled, validHPScaled)
#Our results:
# 400, 0.400 0.100 -> 4.5017, 4.0547
# 400, 0.400 0.200 -> 4.4509, 4.0288
# 400, 0.400 0.400 -> 4.4434, 4.1154
# 400, 0.400 0.500 -> 4.4059, 4.1588
# 400, 0.400 0.600 -> 4.4106, 4.1647
# 400, 0.400 0.800 -> 4.3930, 4.1245
# 1000, 0.400 0.100 -> 4.2904, 4.0562
# 1000, 0.400 0.200 -> 4.2470, 4.0363
# 1000, 0.400 0.400 -> 4.2490, 4.0181
# 1000, 0.400 0.500 -> 4.2201, 4.0372
# 1000, 0.400 0.600 -> 4.2209, 4.0357
# 1000, 0.400 0.800 -> 4.2139, 4.0173
# 2000, 0.400 0.100 -> 4.1367, 3.9843
# 2000, 0.400 0.200 -> 4.1030, 3.9847
# 2000, 0.400 0.400 -> 4.1129, 3.9736
# 2000, 0.400 0.500 -> 4.0934, 3.9652
# 2000, 0.400 0.600 -> 4.0926, 3.9849
# 2000, 0.400 0.800 -> 4.0893, 3.9793
# 3000, 0.400 0.100 -> 4.0677, 3.9342
# 3000, 0.400 0.200 -> 4.0366, 4.0256
# 3000, 0.400 0.400 -> 4.0408, 3.9815
# 3000, 0.400 0.500 -> 4.0282, 3.9833
# 3000, 0.400 0.600 -> 4.0271, 3.9863
# 3000, 0.400 0.800 -> 4.0253, 3.9754
# 5000, 0.400 0.100 -> 3.9826, 3.9597
# 5000, 0.400 0.200 -> 3.9653, 4.0212
# 5000, 0.400 0.400 -> 3.9654, 3.9801
# 5000, 0.400 0.500 -> 3.9600, 3.9780
# 5000, 0.400 0.600 -> 3.9591, 3.9774
# 5000, 0.400 0.800 -> 3.9585, 3.9761
# 10000, 0.400 0.100 -> 3.9020, 3.9701
# 10000, 0.400 0.200 -> 3.8927, 4.0307
# 10000, 0.400 0.400 -> 3.8920, 3.9958
# 10000, 0.400 0.500 -> 3.8900, 4.0092
# 10000, 0.400 0.600 -> 3.8895, 4.0061
# 10000, 0.400 0.800 -> 3.8895, 4.0199

