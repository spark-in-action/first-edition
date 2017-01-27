from __future__ import print_function

#section 8.2.2
def toDoubleSafe(v):
    try:
        return float(v)
    except ValueError:
        return v

census_raw = sc.textFile("first-edition/ch08/adult.raw", 4).map(lambda x:  x.split(", "))
census_raw = census_raw.map(lambda row:  [toDoubleSafe(x) for x in row])

from pyspark.sql.types import *
columns = ["age", "workclass", "fnlwgt", "education", "marital_status",
    "occupation", "relationship", "race", "sex", "capital_gain", "capital_loss",
    "hours_per_week", "native_country", "income"]
adultschema = StructType([
    StructField("age",DoubleType(),True),
    StructField("capital_gain",DoubleType(),True),
    StructField("capital_loss",DoubleType(),True),
    StructField("education",StringType(),True),
    StructField("fnlwgt",DoubleType(),True),
    StructField("hours_per_week",DoubleType(),True),
    StructField("income",StringType(),True),
    StructField("marital_status",StringType(),True),
    StructField("native_country",StringType(),True),
    StructField("occupation",StringType(),True),
    StructField("race",StringType(),True),
    StructField("relationship",StringType(),True),
    StructField("sex",StringType(),True),
    StructField("workclass",StringType(),True)
])
from pyspark.sql import Row
dfraw = sqlContext.createDataFrame(census_raw.map(lambda row: Row(**{x[0]: x[1] for x in zip(columns, row)})), adultschema)
dfraw.show()

dfraw.groupBy(dfraw["workclass"]).count().foreach(print)
#Missing data imputation
dfrawrp = dfraw.na.replace(["?"], ["Private"], ["workclass"])
dfrawrpl = dfrawrp.na.replace(["?"], ["Prof-specialty"], ["occupation"])
dfrawnona = dfrawrpl.na.replace(["?"], ["United-States"], ["native_country"])

#converting strings to numeric values
def indexStringColumns(df, cols):
    from pyspark.ml.feature import StringIndexer
    #variable newdf will be updated several times
    newdf = df
    for c in cols:
        si = StringIndexer(inputCol=c, outputCol=c+"-num")
        sm = si.fit(newdf)
        newdf = sm.transform(newdf).drop(c)
        newdf = newdf.withColumnRenamed(c+"-num", c)
    return newdf

dfnumeric = indexStringColumns(dfrawnona, ["workclass", "education", "marital_status", "occupation", "relationship", "race", "sex", "native_country", "income"])

def oneHotEncodeColumns(df, cols):
    from pyspark.ml.feature import OneHotEncoder
    newdf = df
    for c in cols:
        onehotenc = OneHotEncoder(inputCol=c, outputCol=c+"-onehot", dropLast=False)
        newdf = onehotenc.transform(newdf).drop(c)
        newdf = newdf.withColumnRenamed(c+"-onehot", c)
    return newdf

dfhot = oneHotEncodeColumns(dfnumeric, ["workclass", "education", "marital_status", "occupation", "relationship", "race", "native_country"])

from pyspark.ml.feature import VectorAssembler
va = VectorAssembler(outputCol="features", inputCols=dfhot.columns[0:-1])
lpoints = va.transform(dfhot).select("features", "income").withColumnRenamed("income", "label")

#section 8.2.3
splits = lpoints.randomSplit([0.8, 0.2])
adulttrain = splits[0].cache()
adultvalid = splits[1].cache()


from pyspark.ml.classification import LogisticRegression
lr = LogisticRegression(regParam=0.01, maxIter=1000, fitIntercept=True)
lrmodel = lr.fit(adulttrain)
lrmodel = lr.setParams(regParam=0.01, maxIter=500, fitIntercept=True).fit(adulttrain)

lrmodel.weights
lrmodel.intercept

#section 8.2.3
validpredicts = lrmodel.transform(adultvalid)

from pyspark.ml.evaluation import BinaryClassificationEvaluator
bceval = BinaryClassificationEvaluator()
bceval.evaluate(validpredicts)
bceval.getMetricName()

bceval.setMetricName("areaUnderPR")
bceval.evaluate(validpredicts)

#section 8.2.5
from pyspark.ml.tuning import CrossValidator
from pyspark.ml.tuning import ParamGridBuilder
cv = CrossValidator().setEstimator(lr).setEvaluator(bceval).setNumFolds(5)
paramGrid = ParamGridBuilder().addGrid(lr.maxIter, [1000]).addGrid(lr.regParam, [0.0001, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5]).build()
cv.setEstimatorParamMaps(paramGrid)
cvmodel = cv.fit(adulttrain)
cvmodel.bestModel.weights
BinaryClassificationEvaluator().evaluate(cvmodel.bestModel.transform(adultvalid))

#section 8.2.6
penschema = StructType([
    StructField("pix1",DoubleType(),True),
    StructField("pix2",DoubleType(),True),
    StructField("pix3",DoubleType(),True),
    StructField("pix4",DoubleType(),True),
    StructField("pix5",DoubleType(),True),
    StructField("pix6",DoubleType(),True),
    StructField("pix7",DoubleType(),True),
    StructField("pix8",DoubleType(),True),
    StructField("pix9",DoubleType(),True),
    StructField("pix10",DoubleType(),True),
    StructField("pix11",DoubleType(),True),
    StructField("pix12",DoubleType(),True),
    StructField("pix13",DoubleType(),True),
    StructField("pix14",DoubleType(),True),
    StructField("pix15",DoubleType(),True),
    StructField("pix16",DoubleType(),True),
    StructField("label",DoubleType(),True)
])
pen_raw = sc.textFile("first-edition/ch08/penbased.dat", 4).map(lambda x:  x.split(", ")).map(lambda row: [float(x) for x in row])

dfpen = sqlContext.createDataFrame(pen_raw.map(Row.fromSeq(_)), penschema)
def parseRow(row):
    d = {("pix"+str(i)): row[i-1] for i in range(1,17)}
    d.update({"label": row[16]})
    return Row(**d)

dfpen = sqlContext.createDataFrame(pen_raw.map(parseRow), penschema)
va = VectorAssembler(outputCol="features", inputCols=dfpen.columns[0:-1])
penlpoints = va.transform(dfpen).select("features", "label")

pensets = penlpoints.randomSplit([0.8, 0.2])
pentrain = pensets[0].cache()
penvalid = pensets[1].cache()

penlr = LogisticRegression(regParam=0.01)

#section 8.2.6
# OneVsRest is not available in Python.

#section 8.3.1
from pyspark.ml.feature import StringIndexer
dtsi = StringIndexer(inputCol="label", outputCol="label-ind")
dtsm = dtsi.fit(penlpoints)
pendtlpoints = dtsm.transform(penlpoints).drop("label").withColumnRenamed("label-ind", "label")

pendtsets = pendtlpoints.randomSplit([0.8, 0.2])
pendttrain = pendtsets[0].cache()
pendtvalid = pendtsets[1].cache()

from pyspark.ml.classification import DecisionTreeClassifier
dt = DecisionTreeClassifier(maxDepth=20)
dtmodel = dt.fit(pendttrain)

# rootNode is not accessible in Python

dtpredicts = dtmodel.transform(pendtvalid)
dtresrdd = dtpredicts.select("prediction", "label").rdd.map(lambda row:  (row.prediction, row.label))

from pyspark.mllib.evaluation import MulticlassMetrics
dtmm = MulticlassMetrics(dtresrdd)
dtmm.precision()
#0.951442968392121
print(dtmm.confusionMatrix())
#DenseMatrix([[ 205.,    0.,    3.,    0.,    0.,    3.,    1.,    0.,    0.,
#                 0.],
#             [   0.,  213.,    0.,    1.,    2.,    1.,    0.,    2.,    0.,
#                 2.],
#             [   0.,    0.,  208.,    0.,    0.,    2.,    0.,    1.,    1.,
#                 0.],
#             [   0.,    1.,    0.,  172.,    3.,    0.,    0.,    0.,    0.,
#                 0.],
#             [   2.,    2.,    1.,    8.,  197.,    0.,    0.,    2.,    3.,
#                 1.],
#             [   1.,    0.,    1.,    0.,    2.,  183.,    0.,    1.,    0.,
#                 1.],
#             [   1.,    0.,    0.,    0.,    0.,    0.,  192.,    1.,    1.,
#                 0.],
#             [   0.,    0.,    0.,    0.,    0.,    0.,    1.,  187.,    5.,
#                 0.],
#             [   0.,    1.,    2.,    0.,    0.,    0.,    1.,    5.,  172.,
#                 4.],
#             [   0.,    0.,    0.,    0.,    3.,    0.,    0.,    2.,    2.,
#               176.]])

#section 8.3.2
from pyspark.ml.classification import RandomForestClassifier
rf = RandomForestClassifier(maxDepth=20)
rfmodel = rf.fit(pendttrain)
# RandomForestModel doesn't expose trees field in Python
rfpredicts = rfmodel.transform(pendtvalid)
rfresrdd = rfpredicts.select("prediction", "label").rdd.map(lambda row:  (row.prediction, row.label))
rfmm = MulticlassMetrics(rfresrdd)
rfmm.precision()
#0.9894640403114979
print(rfmm.confusionMatrix())
#DenseMatrix([[ 211.,    0.,    1.,    0.,    0.,    0.,    0.,    0.,    0.,
#                 0.],
#             [   0.,  220.,    0.,    1.,    0.,    0.,    0.,    0.,    0.,
#                 0.],
#             [   0.,    1.,  211.,    0.,    0.,    0.,    0.,    0.,    0.,
#                 0.],
#             [   0.,    0.,    0.,  175.,    1.,    0.,    0.,    0.,    0.,
#                 0.],
#             [   0.,    0.,    0.,    5.,  208.,    1.,    0.,    0.,    0.,
#                 2.],
#             [   0.,    0.,    0.,    0.,    0.,  189.,    0.,    0.,    0.,
#                 0.],
#             [   0.,    1.,    0.,    0.,    0.,    0.,  194.,    0.,    0.,
#                 0.],
#             [   0.,    0.,    0.,    0.,    0.,    0.,    0.,  193.,    0.,
#                 0.],
#             [   0.,    0.,    0.,    0.,    0.,    0.,    2.,    2.,  178.,
#                 3.],
#             [   0.,    0.,    0.,    0.,    0.,    0.,    0.,    1.,    1.,
#               181.]])

#section 8.4.1
from pyspark.mllib.linalg import DenseVector
penflrdd = penlpoints.rdd.map(lambda row: (DenseVector(row.features.toArray()), row.label))
penrdd = penflrdd.map(lambda x:  x[0]).cache()

from pyspark.mllib.clustering import KMeans
kmmodel = KMeans.train(penrdd, 10, maxIterations=5000, runs=20)

kmmodel.computeCost(penrdd)
#44421031.53094221
import math
math.sqrt(kmmodel.computeCost(penrdd)/penrdd.count())
#66.94431052265858

kmpredicts = penflrdd.map(lambda feat_lbl: (float(kmmodel.predict(feat_lbl[0])), feat_lbl[1]))

#rdd contains tuples (prediction, label)
def printContingency(rdd, labels):
    import operator
    numl = len(labels)
    tablew = 6*numl + 10
    divider = "----------"
    for l in labels:
        divider += "+-----"
    summ = 0L
    print("orig.class", end='')
    for l in labels:
        print("|Pred"+str(l), end='')
    print()
    print(divider)
    labelMap = {}
    for l in labels:
        #filtering by predicted labels
        predCounts = rdd.filter(lambda p:  p[1] == l).countByKey()
        #get the cluster with most elements
        topLabelCount = sorted(predCounts.items(), key=operator.itemgetter(1), reverse=True)[0]
        #if there are two (or more) clusters for the same label
        if(topLabelCount[0] in labelMap):
            #and the other cluster has fewer elements, replace it
            if(labelMap[topLabelCount[0]][1] < topLabelCount[1]):
                summ -= labelMap[l][1]
                labelMap.update({topLabelCount[0]: (l, topLabelCount[1])})
                summ += topLabelCount[1]
            #else leave the previous cluster in
        else:
            labelMap.update({topLabelCount[0]: (l, topLabelCount[1])})
            summ += topLabelCount[1]
        predictions = iter(sorted(predCounts.items(), key=operator.itemgetter(0)))
        predcount = next(predictions)
        print("%6d    " % (l), end='')
        for predl in labels:
            if(predcount[0] == predl):
                print("|%5d" % (predcount[1]), end='')
                try:
                    predcount = next(predictions)
                except:
                    pass
            else:
                print("|    0", end='')
        print()
        print(divider)
    print("Purity: %s" % (float(summ)/rdd.count()))
    print("Predicted->original label map: %s" % str([str(x[0])+": "+str(x[1][0]) for x in labelMap.items()]))

printContingency(kmpredicts, range(0, 10))
#orig.class|Pred0|Pred1|Pred2|Pred3|Pred4|Pred5|Pred6|Pred7|Pred8|Pred9
#----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
#     0    |    1|  638|    0|    7|    0|  352|   14|    2|   23|    0
#----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
#     1    |   66|    0|    0|    1|   70|    0|    8|  573|    0|  304
#----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
#     2    |    0|    0|    0|    0|    0|    0|    0|   16|    0| 1006
#----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
#     3    |    2|    0|    0|    1|  919|    0|    0|   19|    0|    1
#----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
#     4    |   31|    0|    0|  940|    1|    0|   42|   12|    0|    1
#----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
#     5    |  175|    0|  555|    0|  211|    0|    6|    0|    3|    0
#----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
#     6    |    0|    0|    1|    3|    0|    0|  965|    0|    0|    0
#----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
#     7    |    0|    0|    4|    1|   70|    0|    1|  147|    1|  805
#----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
#     8    |    4|   15|   22|    0|   98|  385|    6|    0|  398|   31
#----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
#     9    |  609|   10|    0|   78|  171|    0|    1|   76|    1|    9
#----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
#Purity: 0.666162227603
#Predicted->original label map: ['0.0: (9, 609)', '1.0: (0, 638)', '2.0: (5, 555)', '3.0: (4, 940)', '4.0: (3, 919)', '6.0: (6, 965)', '7.0: (1, 573)', '8.0: (8, 398)', '9.0: (2, 1006)']

