package org.sia;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.classification.OneVsRest;
import org.apache.spark.ml.classification.OneVsRestModel;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tree.ContinuousSplit;
import org.apache.spark.ml.tree.InternalNode;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class Chapter08App {
	public static void main(String[] args) throws IOException {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf());
		HiveContext sqlContext = new HiveContext(sc);

		JavaRDD<Object[]> census_raw = sc.textFile("first-edition/ch08/adult.raw", 4).map((x) -> {
			String[] xspl = x.split(", ");
			Object[] xdbl = new Object[xspl.length];
			for (int i = 0; i < xspl.length; i++)
				try {
					xdbl[i] = Double.parseDouble(xspl[i]);
				} catch (Exception e) {
					xdbl[i] = xspl[i];
				}
			return xdbl;
		});

		StructField[] schemaFields = { DataTypes.createStructField("age", DataTypes.DoubleType, true),
				DataTypes.createStructField("workclass", DataTypes.StringType, true),
				DataTypes.createStructField("fnlwgt", DataTypes.DoubleType, true),
				DataTypes.createStructField("education", DataTypes.StringType, true),
				DataTypes.createStructField("marital_status", DataTypes.StringType, true),
				DataTypes.createStructField("occupation", DataTypes.StringType, true),
				DataTypes.createStructField("relationship", DataTypes.StringType, true),
				DataTypes.createStructField("race", DataTypes.StringType, true),
				DataTypes.createStructField("sex", DataTypes.StringType, true),
				DataTypes.createStructField("capital_gain", DataTypes.DoubleType, true),
				DataTypes.createStructField("capital_loss", DataTypes.DoubleType, true),
				DataTypes.createStructField("hours_per_week", DataTypes.DoubleType, true),
				DataTypes.createStructField("native_country", DataTypes.StringType, true),
				DataTypes.createStructField("income", DataTypes.StringType, true) };

		StructType adultschema = DataTypes.createStructType(schemaFields);

		DataFrame dfraw = sqlContext.createDataFrame(census_raw.map((Object[] x) -> RowFactory.create(x[0], x[1], x[2],
				x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10], x[11], x[12], x[13])).rdd(), adultschema);
		dfraw.show();

		dfraw.groupBy(dfraw.col("workclass")).count().toJavaRDD().foreach((x) -> System.out.println(x));
		// Missing data imputation
		Map<String, String> fieldValues = new HashMap<String, String>();
		fieldValues.put("?", "Private");
		DataFrame dfrawrp = dfraw.na().replace("workclass", fieldValues);
		fieldValues.put("?", "Prof-specialty");
		DataFrame dfrawrpl = dfrawrp.na().replace("occupation", fieldValues);
		fieldValues.put("?", "United-States");
		DataFrame dfrawnona = dfrawrpl.na().replace("native_country", fieldValues);

		// converting strings to numeric values
		String[] fields = { "workclass", "education", "marital_status", "occupation", "relationship", "race", "sex",
				"native_country", "income" };
		DataFrame dfnumeric = indexStringColumns(dfrawnona, fields);
		String[] fields2 = { "workclass", "education", "marital_status", "occupation", "relationship", "race",
				"native_country" };
		DataFrame dfhot = oneHotEncodeColumns(dfnumeric, fields2);

		VectorAssembler va = new VectorAssembler().setOutputCol("features");
		List<String> cols = Arrays.asList(dfhot.columns());
		cols.remove("income");
		va.setInputCols(cols.toArray(new String[0]));
		DataFrame lpoints = va.transform(dfhot).select("features", "income").withColumnRenamed("income", "label");

		// section 8.2.3
		double[] splitWeights = { 0.8, 0.2 };
		DataFrame[] splits = lpoints.randomSplit(splitWeights);
		DataFrame adulttrain = splits[0].cache();
		DataFrame adultvalid = splits[1].cache();

		LogisticRegression lr = new LogisticRegression();
		lr.setRegParam(0.01).setMaxIter(1000).setFitIntercept(true);
		LogisticRegressionModel lrmodel = lr.fit(adulttrain);
		ParamMap paramMap = new ParamMap();
		paramMap.put(lr.regParam(), Double.valueOf(0.01));
		paramMap.put(lr.maxIter(), 500);
		paramMap.put(lr.fitIntercept(), Boolean.TRUE);
		LogisticRegressionModel lrmodel2 = lr.fit(adulttrain, paramMap);

		lrmodel.coefficients();
		lrmodel.intercept();

		// section 8.2.3
		DataFrame validpredicts = lrmodel.transform(adultvalid);
		BinaryClassificationEvaluator bceval = new BinaryClassificationEvaluator();
		bceval.evaluate(validpredicts);
		bceval.getMetricName();

		bceval.setMetricName("areaUnderPR");
		bceval.evaluate(validpredicts);

		computePRCurve(adulttrain, adultvalid, lrmodel);

		computeROCCurve(adulttrain, adultvalid, lrmodel);

		// section 8.2.5
		CrossValidator cv = new CrossValidator().setEstimator(lr).setEvaluator(bceval).setNumFolds(5);
		double[] grid = { 0.0001, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5 };
		int[] maxIters = { 1000 };
		ParamMap[] paramGrid = new ParamGridBuilder().addGrid(lr.maxIter(), maxIters).addGrid(lr.regParam(), grid)
				.build();
		cv.setEstimatorParamMaps(paramGrid);
		CrossValidatorModel cvmodel = cv.fit(adulttrain);
		((LogisticRegressionModel) cvmodel.bestModel()).weights();
		((LogisticRegression) cvmodel.bestModel().parent()).getRegParam();
		new BinaryClassificationEvaluator().evaluate(cvmodel.bestModel().transform(adultvalid));

		// section 8.2.6
		StructField[] penschemaFields = { DataTypes.createStructField("pix1", DataTypes.DoubleType, true),
				DataTypes.createStructField("pix2", DataTypes.DoubleType, true),
				DataTypes.createStructField("pix3", DataTypes.DoubleType, true),
				DataTypes.createStructField("pix4", DataTypes.DoubleType, true),
				DataTypes.createStructField("pix5", DataTypes.DoubleType, true),
				DataTypes.createStructField("pix6", DataTypes.DoubleType, true),
				DataTypes.createStructField("pix7", DataTypes.DoubleType, true),
				DataTypes.createStructField("pix8", DataTypes.DoubleType, true),
				DataTypes.createStructField("pix9", DataTypes.DoubleType, true),
				DataTypes.createStructField("pix10", DataTypes.DoubleType, true),
				DataTypes.createStructField("pix11", DataTypes.DoubleType, true),
				DataTypes.createStructField("pix12", DataTypes.DoubleType, true),
				DataTypes.createStructField("pix13", DataTypes.DoubleType, true),
				DataTypes.createStructField("pix14", DataTypes.DoubleType, true),
				DataTypes.createStructField("pix15", DataTypes.DoubleType, true),
				DataTypes.createStructField("pix16", DataTypes.DoubleType, true),
				DataTypes.createStructField("label", DataTypes.DoubleType, true) };

		StructType penschema = DataTypes.createStructType(penschemaFields);

		JavaRDD<Object[]> pen_raw = sc.textFile("first-edition/ch08/penbased.dat", 4).map((x) -> x.split(", "))
				.map((row) -> {
					Object[] mapped = new Object[row.length];
					for (int i = 0; i < row.length; i++)
						mapped[i] = Double.parseDouble(row[i]);
					return mapped;
				});

		DataFrame dfpen = sqlContext.createDataFrame(pen_raw.map((Object[] x) -> RowFactory.create(x[0], x[1], x[2],
				x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10], x[11], x[12], x[13], x[14], x[15], x[16])).rdd(),
				penschema);
		VectorAssembler vapen = new VectorAssembler().setOutputCol("features");
		List<String> dfpencols = Arrays.asList(dfpen.columns());
		dfpencols.remove("label");
		vapen.setInputCols(dfpencols.toArray(new String[0]));
		DataFrame penlpoints = vapen.transform(dfpen).select("features", "label");

		DataFrame[] pensets = penlpoints.randomSplit(splitWeights);
		DataFrame pentrain = pensets[0].cache();
		DataFrame penvalid = pensets[1].cache();

		LogisticRegression penlr = new LogisticRegression().setRegParam(0.01);
		OneVsRest ovrest = new OneVsRest();
		ovrest.setClassifier(penlr);

		OneVsRestModel ovrestmodel = ovrest.fit(pentrain);

		DataFrame penresult = ovrestmodel.transform(penvalid);
		RDD<Tuple2<Object, Object>> penPreds = penresult.select("prediction", "label").toJavaRDD()
				.map((row) -> new Tuple2<Object, Object>(row.getDouble(0), row.getDouble(1))).rdd();

		MulticlassMetrics penmm = new MulticlassMetrics(penPreds);
		penmm.precision();
		penmm.precision(3);
		penmm.recall(3);
		penmm.fMeasure(3);
		penmm.confusionMatrix();

		// section 8.3.1
		StringIndexer dtsi = new StringIndexer().setInputCol("label").setOutputCol("label-ind");
		StringIndexerModel dtsm = dtsi.fit(penlpoints);
		DataFrame pendtlpoints = dtsm.transform(penlpoints).drop("label").withColumnRenamed("label-ind", "label");

		DataFrame[] pendtsets = pendtlpoints.randomSplit(splitWeights);
		DataFrame pendttrain = pendtsets[0].cache();
		DataFrame pendtvalid = pendtsets[1].cache();

		DecisionTreeClassifier dt = new DecisionTreeClassifier();
		dt.setMaxDepth(20);
		DecisionTreeClassificationModel dtmodel = dt.fit(pendttrain);

		dtmodel.rootNode();
		((InternalNode) dtmodel.rootNode()).split().featureIndex();

		((ContinuousSplit) ((InternalNode) dtmodel.rootNode()).split()).threshold();

		((InternalNode) dtmodel.rootNode()).leftChild();
		((InternalNode) dtmodel.rootNode()).rightChild();

		DataFrame dtpredicts = dtmodel.transform(pendtvalid);
		MulticlassMetrics dtmm = new MulticlassMetrics(dtpredicts.select("prediction", "label"));
		dtmm.precision();
		dtmm.confusionMatrix();

		// section 8.3.2
		RandomForestClassifier rf = new RandomForestClassifier();
		rf.setMaxDepth(20);
		RandomForestClassificationModel rfmodel = rf.fit(pendttrain);
		rfmodel.trees();
		DataFrame rfpredicts = rfmodel.transform(pendtvalid);
		MulticlassMetrics rfmm = new MulticlassMetrics(rfpredicts.select("prediction", "label"));
		rfmm.precision();
		rfmm.confusionMatrix();

		// section 8.4.1
		JavaRDD<Tuple2<Vector, Double>> penflrdd = penlpoints.javaRDD()
				.map((Row row) -> new Tuple2((Vector) row.apply(0), (Double) row.apply(1)));
		JavaRDD<Vector> penrdd = penflrdd.map((x) -> x._1).cache();
		KMeansModel kmmodel = KMeans.train(penrdd.rdd(), 10, 5000, 20);

		kmmodel.computeCost(penrdd.rdd());
		Math.sqrt(kmmodel.computeCost(penrdd.rdd()) / penrdd.count());

		JavaPairRDD<Double, Double> kmpredicts = penflrdd
				.mapToPair((feat_lbl) -> new Tuple2((double) kmmodel.predict(feat_lbl._1), feat_lbl._2));

		int[] labels = { 0, 1, 2, 3, 4, 5, 6, 7, 8 };
		printContingency(kmpredicts, labels);

		sc.close();
	}

	public static DataFrame indexStringColumns(DataFrame df, String[] cols) {
		// variable newdf will be updated several times
		DataFrame newdf = df;
		for (String c : cols) {
			StringIndexer si = new StringIndexer().setInputCol(c).setOutputCol(c + "-num");
			StringIndexerModel sm = si.fit(newdf);
			newdf = sm.transform(newdf).drop(c);
			newdf = newdf.withColumnRenamed(c + "-num", c);
		}
		return newdf;
	}

	public static DataFrame oneHotEncodeColumns(DataFrame df, String[] cols) {
		DataFrame newdf = df;
		for (String c : cols) {
			OneHotEncoder onehotenc = new OneHotEncoder().setInputCol(c);
			onehotenc.setOutputCol(c + "-onehot").setDropLast(false);
			newdf = onehotenc.transform(newdf).drop(c);
			newdf = newdf.withColumnRenamed(c + "-onehot", c);
		}
		return newdf;
	}

	public static void computePRCurve(DataFrame train, DataFrame valid, LogisticRegressionModel lrmodel) {
		for (int threshold = 0; threshold < 10; threshold++) {
			double thr = threshold / 10.;
			if (threshold == 10)
				thr -= 0.001;
			lrmodel.setThreshold(thr);
			DataFrame validpredicts = lrmodel.transform(valid);
			JavaRDD<Tuple2<Object, Object>> validPredRdd = validpredicts.toJavaRDD()
					.map((Row row) -> new Tuple2(row.getDouble(4), row.getDouble(1)));
			BinaryClassificationMetrics bcm = new BinaryClassificationMetrics(validPredRdd.rdd());
			Tuple2<Object, Object> pr = ((Tuple2<Object, Object>[]) bcm.pr().collect())[0];
			System.out.println(String.format("%.1f: R=%f, P=%f", thr, pr._1, pr._2));
		}
	}

	public static void computeROCCurve(DataFrame train, DataFrame valid, LogisticRegressionModel lrmodel) {
		for (int threshold = 0; threshold < 10; threshold++) {
			double thr = threshold / 10.;
			if (threshold == 10)
				thr -= 0.001;
			lrmodel.setThreshold(thr);
			DataFrame validpredicts = lrmodel.transform(valid);
			JavaRDD<Tuple2<Object, Object>> validPredRdd = validpredicts.toJavaRDD()
					.map((Row row) -> new Tuple2(row.getDouble(4), row.getDouble(1)));
			BinaryClassificationMetrics bcm = new BinaryClassificationMetrics(validPredRdd.rdd());
			Tuple2<Object, Object> pr = ((Tuple2<Object, Object>[]) bcm.roc().collect())[0];
			System.out.println(String.format("%.1f: FPR=%f, TPR=%f", thr, pr._1, pr._2));
		}
	}

	// rdd contains tuples (prediction, label)
	public static void printContingency(JavaPairRDD<Double, Double> rdd, int[] labels) {
		int numl = labels.length;
		int tablew = 6 * numl + 10;
		String divider = "----------";
		for (int l : labels)
			divider += "+-----";

		long sum = 0;
		System.out.print("orig.class");
		for (int l : labels)
			System.out.print("|Pred" + l);
		System.out.println();
		System.out.println(divider);
		Map<Double, Tuple2<Double, Long>> labelMap = new HashMap<Double, Tuple2<Double, Long>>();
		
		for (int l : labels) {
			// filtering by predicted labels
			Map<Double, Object> predCounts = rdd.filter((p) -> p._2 == l).countByKey();
			// get the cluster with most elements
			double topCountLabel = 0;
			long topCount = 0;
			Iterator<Double> it = predCounts.keySet().iterator();
			while (it.hasNext()) {
				double label = it.next();
				if (topCount < label) {
					topCountLabel = label;
					topCount = (Long) predCounts.get(label);
				}
			}
			// val topLabelCount = predCounts.sortBy{-_._2}.apply(0);
			// if there are two (or more) clusters for the same label
			if (labelMap.containsKey(topCountLabel)) {
				// and the other cluster has fewer elements, replace it
				if (labelMap.get(topCountLabel)._2 < topCount) {
					sum -= labelMap.get(l)._2;
					labelMap.put(topCountLabel, new Tuple2(l, topCount));
					sum += topCount;
				}
				// else leave the previous cluster in
			} else {
				labelMap.put(topCountLabel, new Tuple2(l, topCount));
				sum += topCount;
			}
			it = new TreeSet<Double>(predCounts.keySet()).iterator();

			double predlabel = it.next();
			long predcount = (Long) predCounts.get(predlabel);
			System.out.print(String.format("%6d    ", l));
			for (int predl : labels) {
				if (predlabel == predl) {
					System.out.print(String.format("|%5d", predcount));
					if (it.hasNext())
						predlabel = it.next();
				} else
					System.out.print("|    0");
			}
			System.out.println();
			System.out.println(divider);
		}
		System.out.println("Purity: " + ((double) sum) / rdd.count());
		System.out.println("Predicted->original label map: " + Arrays.asList(labelMap));
	}
}
