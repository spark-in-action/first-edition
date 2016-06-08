// section 14.2.2
import org.apache.spark.h2o._
val h2oContext = new H2OContext(sc).start()

//section 14.2.3
h2oContext.stop(false)

//section 14.3.1
import h2oContext._

val housingLines = sc.textFile("first-edition/ch07/housing.data", <number_of_partitions>)
val housingVals = housingLines.map(x => x.split(",").map(_.trim().toDouble))
import org.apache.spark.sql.types.{StructType,StructField,StringType,DoubleType}
val housingSchema = StructType(Array(
    StructField("crim",DoubleType,true),
    StructField("zn",DoubleType,true),
    StructField("indus",DoubleType,true),
    StructField("chas",DoubleType,true),
    StructField("nox",DoubleType,true),
    StructField("rm",DoubleType,true),
    StructField("age",DoubleType,true),
    StructField("dis",DoubleType,true),
    StructField("rad",DoubleType,true),
    StructField("tax",DoubleType,true),
    StructField("ptratio",DoubleType,true),
    StructField("b",DoubleType,true),
    StructField("lstat",DoubleType,true),
    StructField("medv",DoubleType,true)
))
import org.apache.spark.sql.Row
val housingDF = sqlContext.applySchema(housingVals.map(Row.fromSeq(_)), housingSchema)

val housingH2o = h2oContext.asH2OFrame(housingDF, "housing")

//section 14.3.3
val housing750 = new H2OFrame("housing750")
val housing250 = new H2OFrame("housing250")

import org.apache.spark.examples.h2o.DemoUtils
val housingSplit = DemoUtils.split(housingH2o, Array("housing750sw", "housing250sw"), Array(0.75))
val housing750 = housingSplit(0)
val housing250 = housingSplit(1)


import _root_.hex.deeplearning.DeepLearningModel.DeepLearningParameters
val dlParams = new DeepLearningParameters()
dlParams._train = housing750._key
dlParams._valid = housing250._key
dlParams._response_column = "medv"
dlParams._epochs = 200
dlParams._l1 = 1e-5
dlParams._hidden = Array(200, 200, 200)

import _root_.hex.deeplearning.DeepLearning
val dlBuildJob = new DeepLearning(dlParams).trainModel
val housingModel = dlBuildJob.get

val housingMetrics = _root_.hex.ModelMetricsRegression.getFromDKV(housingModel, housing250)

val housingValPreds = housingModel.score(housing250)
val housingPredictions = h2oContext.asDataFrame(housingValPreds)(sqlContext)
housingPredictions.show()

import water.app.ModelSerializationSupport
import _root_.hex.deeplearning.DeepLearningModel
ModelSerializationSupport.exportH2OModel(housingModel, new java.net.URI("file:///path/to/model/file"))

val modelImported:DeepLearningModel = ModelSerializationSupport.loadH2OModel(new java.net.URI("file:///path/to/model/file"))


//section 14.4.1
val censusH2o = new H2OFrame(new java.net.URI("first-edition/ch08/adult.raw"))

censusH2o.setNames(Array("age1", "workclass", "fnlwgt", "education", "marital_status", "occupation", "relationship", "race", "sex", "capital_gain", "capital_loss", "hours_per_week", "native_country", "income"))
censusH2o.name(0)
censusH2o.update()


import org.apache.spark.examples.h2o.DemoUtils
val censusSplit = DemoUtils.split(censusH2o, Array("census750", "census250"), Array(0.75))
val census750 = censusSplit(0)
val census250 = censusSplit(1)


//section 14.4.3
import _root_.hex.deeplearning.DeepLearningModel.DeepLearningParameters
val censusdlParams = new DeepLearningParameters()
censusdlParams._train = census750._key
censusdlParams._valid = census250._key
censusdlParams._response_column = "income"
censusdlParams._epochs = 50
censusdlParams._l1 = 1e-5
censusdlParams._hidden = Array(200, 200, 200)

import _root_.hex.deeplearning.DeepLearning
val censusModel = new DeepLearning(censusdlParams).trainModel.get

val censusPredictions = censusModel.score(census250)
val censusMetrics = _root_.hex.ModelMetricsBinomial.getFromDKV(censusModel, census250)

