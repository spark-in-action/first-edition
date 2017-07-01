
import spark.implicits._

//section 8.2.2
val census_raw = sc.textFile("first-edition/ch08/adult.raw", 4).map(x => x.split(",").map(_.trim)).
    map(row => row.map(x => try { x.toDouble } catch { case _ : Throwable => x }))

import org.apache.spark.sql.types.{StructType,StructField,StringType,DoubleType}
val adultschema = StructType(Array(
    StructField("age",DoubleType,true),
    StructField("workclass",StringType,true),
    StructField("fnlwgt",DoubleType,true),
    StructField("education",StringType,true),
    StructField("marital_status",StringType,true),
    StructField("occupation",StringType,true),
    StructField("relationship",StringType,true),
    StructField("race",StringType,true),
    StructField("sex",StringType,true),
    StructField("capital_gain",DoubleType,true),
    StructField("capital_loss",DoubleType,true),
    StructField("hours_per_week",DoubleType,true),
    StructField("native_country",StringType,true),
    StructField("income",StringType,true)
))
import org.apache.spark.sql.Row
val dfraw = spark.createDataFrame(census_raw.map(Row.fromSeq(_)), adultschema)
dfraw.show()

dfraw.groupBy(dfraw("workclass")).count().rdd.foreach(println)
//Missing data imputation
val dfrawrp = dfraw.na.replace(Array("workclass"), Map("?" -> "Private"))
val dfrawrpl = dfrawrp.na.replace(Array("occupation"), Map("?" -> "Prof-specialty"))
val dfrawnona = dfrawrpl.na.replace(Array("native_country"), Map("?" -> "United-States"))

//converting strings to numeric values
import org.apache.spark.sql.DataFrame
def indexStringColumns(df:DataFrame, cols:Array[String]):DataFrame = {
    import org.apache.spark.ml.feature.StringIndexer
    import org.apache.spark.ml.feature.StringIndexerModel
    //variable newdf will be updated several times
    var newdf = df
    for(c <- cols) {
        val si = new StringIndexer().setInputCol(c).setOutputCol(c+"-num")
        val sm:StringIndexerModel = si.fit(newdf)
        newdf = sm.transform(newdf).drop(c)
        newdf = newdf.withColumnRenamed(c+"-num", c)
    }
    newdf
}
val dfnumeric = indexStringColumns(dfrawnona, Array("workclass", "education", "marital_status", "occupation", "relationship", "race", "sex", "native_country", "income"))

def oneHotEncodeColumns(df:DataFrame, cols:Array[String]):DataFrame = {
    import org.apache.spark.ml.feature.OneHotEncoder
    var newdf = df
    for(c <- cols) {
        val onehotenc = new OneHotEncoder().setInputCol(c)
        onehotenc.setOutputCol(c+"-onehot").setDropLast(false)
        newdf = onehotenc.transform(newdf).drop(c)
        newdf = newdf.withColumnRenamed(c+"-onehot", c)
    }
    newdf
}
val dfhot = oneHotEncodeColumns(dfnumeric, Array("workclass", "education", "marital_status", "occupation", "relationship", "race", "native_country"))

import org.apache.spark.ml.feature.VectorAssembler
val va = new VectorAssembler().setOutputCol("features")
va.setInputCols(dfhot.columns.diff(Array("income")))
val lpoints = va.transform(dfhot).select("features", "income").withColumnRenamed("income", "label")

//section 8.2.3
val splits = lpoints.randomSplit(Array(0.8, 0.2))
val adulttrain = splits(0).cache()
val adultvalid = splits(1).cache()


import org.apache.spark.ml.classification.LogisticRegression
val lr = new LogisticRegression
lr.setRegParam(0.01).setMaxIter(1000).setFitIntercept(true)
val lrmodel = lr.fit(adulttrain)
import org.apache.spark.ml.param.ParamMap
val lrmodel = lr.fit(adulttrain, ParamMap(lr.regParam -> 0.01, lr.maxIter -> 500, lr.fitIntercept -> true))

lrmodel.coefficients
lrmodel.intercept

//section 8.2.3
val validpredicts = lrmodel.transform(adultvalid)
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
val bceval = new BinaryClassificationEvaluator()
bceval.evaluate(validpredicts)
bceval.getMetricName

bceval.setMetricName("areaUnderPR")
bceval.evaluate(validpredicts)

import org.apache.spark.ml.classification.LogisticRegressionModel
def computePRCurve(train:DataFrame, valid:DataFrame, lrmodel:LogisticRegressionModel) =
{
    import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
    for(threshold <- 0 to 10)
    {
        var thr = threshold/10.0
        if(threshold == 10)
            thr -= 0.001
        lrmodel.setThreshold(thr)
        val validpredicts = lrmodel.transform(valid)
        val validPredRdd = validpredicts.rdd.map(row => (row.getDouble(4), row.getDouble(1)))
        val bcm = new BinaryClassificationMetrics(validPredRdd)
        val pr = bcm.pr.collect()(1)
        println("%.1f: R=%f, P=%f".format(thr, pr._1, pr._2))
    }
}
computePRCurve(adulttrain, adultvalid, lrmodel)
// 0.0: R=1.000000, P=0.238081
// 0.1: R=0.963706, P=0.437827
// 0.2: R=0.891973, P=0.519135
// 0.3: R=0.794620, P=0.592486
// 0.4: R=0.694278, P=0.680905
// 0.5: R=0.578992, P=0.742200
// 0.6: R=0.457728, P=0.807837
// 0.7: R=0.324936, P=0.850279
// 0.8: R=0.202818, P=0.920543
// 0.9: R=0.084543, P=0.965854
// 1.0: R=0.019214, P=1.000000
def computeROCCurve(train:DataFrame, valid:DataFrame, lrmodel:LogisticRegressionModel) =
{
    import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
    for(threshold <- 0 to 10)
    {
        var thr = threshold/10.0
        if(threshold == 10)
            thr -= 0.001
        lrmodel.setThreshold(thr)
        val validpredicts = lrmodel.transform(valid)
        val validPredRdd = validpredicts.rdd.map(row => (row.getDouble(4), row.getDouble(1)))
        val bcm = new BinaryClassificationMetrics(validPredRdd)
        val pr = bcm.roc.collect()(1)
        println("%.1f: FPR=%f, TPR=%f".format(thr, pr._1, pr._2))
    }
}
computeROCCurve(adulttrain, adultvalid, lrmodel)
// 0,0: R=1,000000, P=0,237891
// 0,1: R=0,953708, P=0,430118
// 0,2: R=0,891556, P=0,515234
// 0,3: R=0,794256, P=0,586950
// 0,4: R=0,672525, P=0,668228
// 0,5: R=0,579511, P=0,735983
// 0,6: R=0,451350, P=0,783482
// 0,7: R=0,330047, P=0,861298
// 0,8: R=0,205315, P=0,926499
// 0,9: R=0,105444, P=0,972332
// 1,0: R=0,027004, P=1,000000

//section 8.2.5
import org.apache.spark.ml.tuning.CrossValidator
val cv = new CrossValidator().setEstimator(lr).setEvaluator(bceval).setNumFolds(5)
import org.apache.spark.ml.tuning.ParamGridBuilder
val paramGrid = new ParamGridBuilder().addGrid(lr.maxIter, Array(1000)).
    addGrid(lr.regParam, Array(0.0001, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5)).build()
cv.setEstimatorParamMaps(paramGrid)
val cvmodel = cv.fit(adulttrain)
cvmodel.bestModel.asInstanceOf[LogisticRegressionModel].coefficients
cvmodel.bestModel.parent.asInstanceOf[LogisticRegression].getRegParam
new BinaryClassificationEvaluator().evaluate(cvmodel.bestModel.transform(adultvalid))

//section 8.2.6
import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType}
val penschema = StructType(Array(
    StructField("pix1",IntegerType,true),
    StructField("pix2",IntegerType,true),
    StructField("pix3",IntegerType,true),
    StructField("pix4",IntegerType,true),
    StructField("pix5",IntegerType,true),
    StructField("pix6",IntegerType,true),
    StructField("pix7",IntegerType,true),
    StructField("pix8",IntegerType,true),
    StructField("pix9",IntegerType,true),
    StructField("pix10",IntegerType,true),
    StructField("pix11",IntegerType,true),
    StructField("pix12",IntegerType,true),
    StructField("pix13",IntegerType,true),
    StructField("pix14",IntegerType,true),
    StructField("pix15",IntegerType,true),
    StructField("pix16",IntegerType,true),
    StructField("label",IntegerType,true)
))
val pen_raw = sc.textFile("first-edition/ch08/penbased.dat", 4).map(x => x.split(", ")).
    map(row => row.map(x => x.toDouble.toInt))

import org.apache.spark.sql.Row
val dfpen = spark.createDataFrame(pen_raw.map(Row.fromSeq(_)), penschema)
import org.apache.spark.ml.feature.VectorAssembler
val va = new VectorAssembler().setOutputCol("features")
va.setInputCols(dfpen.columns.diff(Array("label")))
val penlpoints = va.transform(dfpen).select("features", "label")

val pensets = penlpoints.randomSplit(Array(0.8, 0.2))
val pentrain = pensets(0).cache()
val penvalid = pensets(1).cache()

val penlr = new LogisticRegression().setRegParam(0.01)
import org.apache.spark.ml.classification.OneVsRest
val ovrest = new OneVsRest()
ovrest.setClassifier(penlr)

val ovrestmodel = ovrest.fit(pentrain)

val penresult = ovrestmodel.transform(penvalid)
val penPreds = penresult.select("prediction", "label").rdd.map(row => (row.getDouble(0), row.getDouble(1)))

import org.apache.spark.mllib.evaluation.MulticlassMetrics
val penmm = new MulticlassMetrics(penPreds)
penmm.precision
//0.9018214127054642
penmm.precision(3)
//0.9026548672566371
penmm.recall(3)
//0.9855072463768116
penmm.fMeasure(3)
//0.9422632794457274
penmm.confusionMatrix
// 228.0  1.0    0.0    0.0    1.0    0.0    1.0    0.0    10.0   1.0
// 0.0    167.0  27.0   3.0    0.0    19.0   0.0    0.0    0.0    0.0
// 0.0    11.0   217.0  0.0    0.0    0.0    0.0    2.0    0.0    0.0
// 0.0    0.0    0.0    204.0  1.0    0.0    0.0    1.0    0.0    1.0
// 0.0    0.0    1.0    0.0    231.0  1.0    2.0    0.0    0.0    2.0
// 0.0    0.0    1.0    9.0    0.0    153.0  9.0    0.0    9.0    34.0
// 0.0    0.0    0.0    0.0    1.0    0.0    213.0  0.0    2.0    0.0
// 0.0    14.0   2.0    6.0    3.0    1.0    0.0    199.0  1.0    0.0
// 7.0    7.0    0.0    1.0    0.0    4.0    0.0    1.0    195.0  0.0
// 1.0    9.0    0.0    3.0    3.0    7.0    0.0    1.0    0.0    223.0


//section 8.3.1
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.StringIndexerModel
val dtsi = new StringIndexer().setInputCol("label").setOutputCol("label-ind")
val dtsm:StringIndexerModel = dtsi.fit(penlpoints)
val pendtlpoints = dtsm.transform(penlpoints).drop("label").withColumnRenamed("label-ind", "label")

val pendtsets = pendtlpoints.randomSplit(Array(0.8, 0.2))
val pendttrain = pendtsets(0).cache()
val pendtvalid = pendtsets(1).cache()

import org.apache.spark.ml.classification.DecisionTreeClassifier
val dt = new DecisionTreeClassifier()
dt.setMaxDepth(20)
val dtmodel = dt.fit(pendttrain)

dtmodel.rootNode
import org.apache.spark.ml.tree.InternalNode
dtmodel.rootNode.asInstanceOf[InternalNode].split.featureIndex
//15
import org.apache.spark.ml.tree.ContinuousSplit
dtmodel.rootNode.asInstanceOf[InternalNode].split.asInstanceOf[ContinuousSplit].threshold
//51
dtmodel.rootNode.asInstanceOf[InternalNode].leftChild
dtmodel.rootNode.asInstanceOf[InternalNode].rightChild

val dtpredicts = dtmodel.transform(pendtvalid)
val dtresrdd = dtpredicts.select("prediction", "label").rdd.map(row => (row.getDouble(0), row.getDouble(1)))
val dtmm = new MulticlassMetrics(dtresrdd)
dtmm.precision
//0.951442968392121
dtmm.confusionMatrix
// 192.0  0.0    0.0    9.0    2.0    0.0    2.0    0.0    0.0    0.0
// 0.0    225.0  0.0    1.0    0.0    1.0    0.0    0.0    3.0    2.0
// 0.0    1.0    217.0  1.0    0.0    1.0    0.0    1.0    1.0    0.0
// 9.0    1.0    0.0    205.0  5.0    1.0    3.0    1.0    1.0    0.0
// 2.0    0.0    1.0    1.0    221.0  0.0    2.0    3.0    0.0    0.0
// 0.0    1.0    0.0    1.0    0.0    201.0  0.0    0.0    0.0    1.0
// 2.0    1.0    0.0    2.0    1.0    0.0    207.0  0.0    2.0    3.0
// 0.0    0.0    3.0    1.0    1.0    0.0    1.0    213.0  1.0    2.0
// 0.0    0.0    0.0    2.0    0.0    2.0    2.0    4.0    198.0  6.0
// 0.0    1.0    0.0    0.0    1.0    0.0    3.0    3.0    4.0    198.0

//section 8.3.2
import org.apache.spark.ml.classification.RandomForestClassifier
val rf = new RandomForestClassifier()
rf.setMaxDepth(20)
val rfmodel = rf.fit(pendttrain)
rfmodel.trees
val rfpredicts = rfmodel.transform(pendtvalid)
val rfresrdd = rfpredicts.select("prediction", "label").rdd.map(row => (row.getDouble(0), row.getDouble(1)))
val rfmm = new MulticlassMetrics(rfresrdd)
rfmm.precision
//0.9894640403114979
rfmm.confusionMatrix
// 205.0  0.0    0.0    0.0    0.0    0.0    0.0    0.0    0.0    0.0
// 0.0    231.0  0.0    0.0    0.0    0.0    0.0    0.0    1.0    0.0
// 0.0    0.0    221.0  1.0    0.0    0.0    0.0    0.0    0.0    0.0
// 5.0    0.0    0.0    219.0  0.0    0.0    2.0    0.0    0.0    0.0
// 0.0    0.0    0.0    0.0    230.0  0.0    0.0    0.0    0.0    0.0
// 0.0    1.0    0.0    0.0    0.0    203.0  0.0    0.0    0.0    0.0
// 1.0    0.0    0.0    1.0    0.0    0.0    216.0  0.0    0.0    0.0
// 0.0    0.0    1.0    0.0    2.0    0.0    0.0    219.0  0.0    0.0
// 0.0    0.0    0.0    1.0    0.0    0.0    0.0    1.0    212.0  0.0
// 0.0    0.0    0.0    0.0    0.0    0.0    2.0    2.0    2.0    204.0


//section 8.4.1
import org.apache.spark.ml.clustering.KMeans
val kmeans = new KMeans()
kmeans.setK(10)
kmeans.setMaxIter(500)
val kmmodel = kmeans.fit(penlpoints)

kmmodel.computeCost(penlpoints)
//4.517530920539787E7
math.sqrt(kmmodel.computeCost(penlpoints)/penlpoints.count())
//67.5102817068467

val kmpredicts = kmmodel.transform(penlpoints)

import org.apache.spark.rdd.RDD
//df has to contain at least two columns named prediction and label
def printContingency(df:org.apache.spark.sql.DataFrame, labels:Seq[Int])
{
    val rdd:RDD[(Int, Int)] = df.select('label, 'prediction).rdd.map(row => (row.getInt(0), row.getInt(1))).cache()
    val numl = labels.size
    val tablew = 6*numl + 10
    var divider = "".padTo(10, '-')
    for(l <- labels)
        divider += "+-----"

    var sum:Long = 0
    print("orig.class")
    for(l <- labels)
        print("|Pred"+l)
    println
    println(divider)
    val labelMap = scala.collection.mutable.Map[Int, (Int, Long)]()
    for(l <- labels)
    {
        //filtering by predicted labels
        val predCounts = rdd.filter(p => p._2 == l).countByKey().toList
        //get the cluster with most elements
        val topLabelCount = predCounts.sortBy{-_._2}.apply(0)
        //if there are two (or more) clusters for the same label
        if(labelMap.contains(topLabelCount._1))
        {
            //and the other cluster has fewer elements, replace it
            if(labelMap(topLabelCount._1)._2 < topLabelCount._2)
            {
                sum -= labelMap(l)._2
                labelMap += (topLabelCount._1 -> (l, topLabelCount._2))
                sum += topLabelCount._2
            }
            //else leave the previous cluster in
        } else
        {
            labelMap += (topLabelCount._1 -> (l, topLabelCount._2))
            sum += topLabelCount._2
        }
        val predictions = predCounts.sortBy{_._1}.iterator
        var predcount = predictions.next()
        print("%6d".format(l)+"    ")
        for(predl <- labels)
        {
            if(predcount._1 == predl)
            {
                print("|%5d".format(predcount._2))
                if(predictions.hasNext)
                    predcount = predictions.next()
            }
            else
                print("|    0")
        }
        println
        println(divider)
    }
    rdd.unpersist()
    println("Purity: "+sum.toDouble/rdd.count())
    println("Predicted->original label map: "+labelMap.mapValues(x => x._1))
}
printContingency(kmpredicts, 0 to 9)
// orig.class|Pred0|Pred1|Pred2|Pred3|Pred4|Pred5|Pred6|Pred7|Pred8|Pred9
// ----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
//      0    |    1|  379|   14|    7|    2|  713|    0|    0|   25|    2
// ----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
//      1    |  333|    0|    9|    1|  642|    0|   88|    0|    0|   70
// ----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
//      2    | 1130|    0|    0|    0|   14|    0|    0|    0|    0|    0
// ----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
//      3    |    1|    0|    0|    1|   24|    0| 1027|    0|    0|    2
// ----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
//      4    |    1|    0|   51| 1046|   13|    0|    1|    0|    0|   32
// ----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
//      5    |    0|    0|    6|    0|    0|    0|  235|  624|    3|  187
// ----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
//      6    |    0|    0| 1052|    3|    0|    0|    0|    1|    0|    0
// ----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
//      7    |  903|    0|    1|    1|  154|    0|   78|    4|    1|    0
// ----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
//      8    |   32|  433|    6|    0|    0|   16|  106|   22|  436|    4
// ----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
//      9    |    9|    0|    1|   88|   82|   11|  199|    0|    1|  664
// ----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
// Purity: 0.6672125181950509
// Predicted->original label map: Map(8 -> 8, 2 -> 6, 5 -> 0, 4 -> 1, 7 -> 5, 9 -> 9, 3 -> 4, 6 -> 3, 0 -> 2)

