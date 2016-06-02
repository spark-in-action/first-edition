package org.sia;

import org.sia.scala.BreezeMethods;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.DenseMatrix;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import org.apache.spark.mllib.regression.RidgeRegressionModel;
import org.apache.spark.mllib.linalg.SparseMatrix;
import org.apache.spark.mllib.optimization.LeastSquaresGradient;
import org.apache.spark.mllib.optimization.SquaredL2Updater;
import org.apache.spark.mllib.optimization.GradientDescent;
import org.apache.spark.mllib.optimization.LBFGS;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.mllib.regression.LassoWithSGD;
import org.apache.spark.mllib.regression.RidgeRegressionWithSGD;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.evaluation.RegressionMetrics;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LassoModel;
import org.apache.spark.mllib.feature.StandardScaler;
import org.apache.spark.mllib.feature.StandardScalerModel;
import org.apache.spark.mllib.linalg.Matrices;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;

import scala.Tuple2;

public class Chapter07App {

	public static void main(String[] args) throws IOException {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf());

		Logger.getLogger("breeze").setLevel(Level.WARN);

		System.out.println("\nSection 7.2.1\n");
		
		Vector dv1 = Vectors.dense(5.0, 6.0, 7.0, 8.0);
		double[] dvvals = { 5.0, 6.0, 7.0, 8.0 };
		Vector dv2 = Vectors.dense(dvvals);
		int[] dvinds = { 0, 1, 2, 3 };
		Vector sv = Vectors.sparse(4, dvinds, dvvals);
		dv2.apply(2);
		dv1.size();
		dv2.toArray();

		double[] dmvals = { 5.0, 0.0, 0.0, 3.0, 1.0, 4.0 };
		Matrix dm = Matrices.dense(2, 3, dmvals);
		double[] smvals = { 5.0, 3.0, 1.0, 4.0 };
		int[] smind1 = { 0, 1, 2, 4 }, smind2 = { 0, 1, 0, 1 };
		Matrix sm = Matrices.sparse(2, 3, smind1, smind2, smvals);
		((SparseMatrix) sm).toDense();
		((DenseMatrix) dm).toSparse();
		dm.apply(1, 1);
		dm.transpose();

		System.out.println("\nSection 7.4\n");
		
		JavaRDD<String> housingLines = sc.textFile("first-edition/ch07/housing.data", 6);
		JavaRDD<Vector> housingVals = housingLines.map((String x) -> {
			String[] arr = x.split(",");
			double[] darr = new double[arr.length];
			for (int i = 0; i < arr.length; i++)
				darr[i] = Double.parseDouble(arr[i].trim());
			return Vectors.dense(darr);
		});

		System.out.println("\nSection 7.4.1\n");
		
		RowMatrix housingMat = new RowMatrix(housingVals.rdd());
		MultivariateStatisticalSummary housingStats = housingMat.computeColumnSummaryStatistics();
		housingStats.min();

		System.out.println("\nSection 7.4.2\n");
		
		CoordinateMatrix housingColSims = housingMat.columnSimilarities();

		printMat(BreezeMethods.toBreezeD(housingColSims));

		System.out.println("\nSection 7.4.3\n");
		
		Matrix housingCovar = housingMat.computeCovariance();
		printMat(BreezeMethods.toBreezeM(housingCovar));

		System.out.println("\nSection 7.4.4\n");
		
		JavaRDD<LabeledPoint> housingData = housingVals.map((Vector x) -> {
			double[] a = x.toArray();
			return new LabeledPoint(a[a.length - 1], Vectors.dense(Arrays.copyOf(a, a.length - 1)));
		});

		System.out.println("\nSection 7.4.5\n");
		
		double[] spltwghts = { 0.8, 0.2 };
		JavaRDD<LabeledPoint>[] sets = housingData.randomSplit(spltwghts);
		JavaRDD<LabeledPoint> housingTrain = sets[0];
		JavaRDD<LabeledPoint> housingValid = sets[1];

		System.out.println("\nSection 7.4.6\n");
		
		StandardScalerModel scaler = new StandardScaler(true, true)
				.fit(housingTrain.map((LabeledPoint x) -> x.features()).rdd());
		JavaRDD<LabeledPoint> trainScaled = housingTrain
				.map((LabeledPoint x) -> new LabeledPoint(x.label(), scaler.transform(x.features())));
		JavaRDD<LabeledPoint> validScaled = housingValid
				.map((LabeledPoint x) -> new LabeledPoint(x.label(), scaler.transform(x.features())));

		System.out.println("\nSection 7.5\n");
		
		LinearRegressionWithSGD alg = new LinearRegressionWithSGD();
		alg.setIntercept(true);
		alg.optimizer().setNumIterations(200);
		trainScaled.cache();
		validScaled.cache();
		LinearRegressionModel model = alg.run(trainScaled.rdd());

		System.out.println("\nSection 7.5.1\n");
		
		JavaRDD<Tuple2<Object, Object>> validPredicts = validScaled
				.map((LabeledPoint x) -> new Tuple2<Object, Object>(model.predict(x.features()), x.label()));
		validPredicts.collect();
		double RMSE = Math.sqrt(validPredicts
				.mapToDouble((Tuple2<Object, Object> pl) -> Math.pow((Double) pl._1 - (Double) pl._2, 2)).mean());

		System.out.println("\nSection 7.5.2\n");
		
		RegressionMetrics validMetrics = new RegressionMetrics(validPredicts.rdd());
		validMetrics.rootMeanSquaredError();
		validMetrics.meanSquaredError();

		System.out.println("\nSection 7.5.3\n");
		
		double[] weights = model.weights().toArray();
		Tuple2[] winds = new Tuple2[weights.length];
		for (int i = 0; i < weights.length; i++)
			winds[i] = new Tuple2(Math.abs(weights[i]), i);
		Arrays.sort(winds, new Comparator<Tuple2>() {
			public int compare(Tuple2 arg0, Tuple2 arg1) {
				return ((Double) arg0._1).compareTo((Double) arg1._2);
			};
		});
		for (Tuple2 x : winds)
			System.out.print("(" + x._1 + ", " + x._2 + "), ");
		System.out.println();

		System.out.println("\nSection 7.5.3\n");
		
		model.save(sc.sc(), "hdfs:///path/to/saved/model");

		LinearRegressionModel loadedmodel = LinearRegressionModel.load(sc.sc(), "hdfs:///path/to/saved/model");

		System.out.println("\nSection 7.6.1\n");
		
		int[] iters = { 200, 400, 600 };
		double[] stepSizes = { 0.05, 0.1, 0.5, 1, 1.5, 2, 3 };
		iterateLRwSGD(iters, stepSizes, trainScaled, validScaled);

		System.out.println("\nSection 7.6.2\n");
		
		JavaRDD<LabeledPoint> housingHP = housingData
				.map((v) -> new LabeledPoint(v.label(), addHighPols(v.features())));

		housingHP.first().features().size();

		double[] hspl = { 0.8, 0.2 };
		JavaRDD<LabeledPoint>[] setsHP = housingHP.randomSplit(hspl);
		JavaRDD<LabeledPoint> housingHPTrain = setsHP[0];
		JavaRDD<LabeledPoint> housingHPValid = setsHP[1];
		StandardScalerModel scalerHP = new StandardScaler(true, true)
				.fit(housingHPTrain.map((x) -> x.features()).rdd());
		JavaRDD<LabeledPoint> trainHPScaled = housingHPTrain
				.map((x) -> new LabeledPoint(x.label(), scalerHP.transform(x.features())));
		JavaRDD<LabeledPoint> validHPScaled = housingHPValid
				.map((x) -> new LabeledPoint(x.label(), scalerHP.transform(x.features())));
		trainHPScaled.cache();
		validHPScaled.cache();

		int[] iters2 = { 200, 400 };
		double[] stepSizes2 = { 0.4, 0.5, 0.6, 0.7, 0.9, 1.0, 1.1, 1.2, 1.3, 1.5 };
		iterateLRwSGD(iters2, stepSizes2, trainHPScaled, validHPScaled);

		int[] iters3 = { 200, 400, 800, 1000, 3000, 6000 };
		double[] stepSizes3 = { 1.1 };
		iterateLRwSGD(iters3, stepSizes3, trainHPScaled, validHPScaled);

		System.out.println("\nSection 7.6.3\n");
		
		int[] iters4 = { 10000, 15000, 30000, 50000 };
		double[] stepSizes4 = stepSizes3;
		iterateLRwSGD(iters4, stepSizes4, trainHPScaled, validHPScaled);

		System.out.println("\nSection 7.6.5\n");
		
		int[] riters = { 200, 400, 1000, 3000, 6000, 10000 };
		double[] rsteps = stepSizes3;
		iterateRidge(riters, rsteps, 0.01, trainHPScaled, validHPScaled);

		int[] liters = { 200, 400, 1000, 3000, 6000, 10000, 15000 };
		double[] lsteps = stepSizes3;
		iterateLasso(liters, lsteps, 0.01, trainHPScaled, validHPScaled);

		System.out.println("\nSection 7.7.1\n");
		
		int[] lrwiters = { 400, 1000 };
		double[] lrwsteps = { 0.05, 0.09, 0.1, 0.15, 0.2, 0.3, 0.35, 0.4, 0.5, 1 };
		double[] lrwfracs = { 0.01, 0.1 };
		iterateLRwSGDBatch(lrwiters, lrwsteps, lrwfracs, trainHPScaled, validHPScaled);

		int[] lrwiters2 = { 400, 1000, 2000, 3000, 5000, 10000 };
		double[] lrwsteps2 = { 0.4 };
		double[] lrwfracs2 = { 0.1, 0.2, 0.4, 0.5, 0.6, 0.8 };
		iterateLRwSGDBatch(lrwiters2, lrwsteps2, lrwfracs2, trainHPScaled, validHPScaled);

		System.out.println("\nSection 7.7.2\n");
		
		double[] lbfgsregs = { 0.005, 0.007, 0.01, 0.02, 0.03, 0.05, 0.1 };
		iterateLBFGS(lbfgsregs, 10, 1e-5, trainHPScaled, validHPScaled);

		sc.close();
	}

	// UTILITY METHOD FOR PRETTY-PRINTING MATRICES
	private static void printMat(breeze.linalg.Matrix<Object> mat) {
		System.out.print("            ");
		for (int j = 0; j < mat.cols(); j++)
			System.out.print(String.format("%-10d", j));
		System.out.println();
		for (int i = 0; i < mat.rows(); i++) {
			System.out.print(String.format("%-6d", i));
			for (int j = 0; j < mat.cols(); j++)
				System.out.print(String.format(" %+9.3f", mat.apply(i, j)));
			System.out.println();
		}
	}

	private static Vector addHighPols(Vector v) {
		double[] a = v.toArray();
		double[] x = new double[a.length * 2];
		for (int i = 0; i < a.length; i++) {
			x[i * 2] = a[i];
			x[i * 2 + 1] = a[i] * a[i];
		}
		return Vectors.dense(x);
	};

	private static void iterateLRwSGD(int[] iterNums, double[] stepSizes, JavaRDD<LabeledPoint> train,
			JavaRDD<LabeledPoint> test) {
		for (int numIter : iterNums) {
			for (double step : stepSizes) {
				LinearRegressionWithSGD alg = new LinearRegressionWithSGD();
				((GradientDescent) alg.setIntercept(true).optimizer()).setNumIterations(numIter).setStepSize(step);
				LinearRegressionModel model = alg.run(train.rdd());
				JavaRDD<Tuple2<Double, Double>> rescaledPredicts = train
						.map((x) -> new Tuple2<Double, Double>(model.predict(x.features()), x.label()));
				JavaRDD<Tuple2<Double, Double>> validPredicts = test
						.map((x) -> new Tuple2<Double, Double>(model.predict(x.features()), x.label()));
				double meanSquared = Math.sqrt(
						rescaledPredicts.mapToDouble((Tuple2<Double, Double> pl) -> Math.pow(pl._1 - pl._2, 2)).mean());
				double meanSquaredValid = Math.sqrt(
						validPredicts.mapToDouble((Tuple2<Double, Double> pl) -> Math.pow(pl._1 - pl._2, 2)).mean());
				System.out.println(
						String.format("%d, %5.3f -> %.4f, %.4f", numIter, step, meanSquared, meanSquaredValid));
				// Uncomment if you wish to see weghts and intercept values:
				// System.out.println("%d, %4.2f -> %.4f, %.4f (%s,
				// %f)".format(numIter, step, meanSquared, meanSquaredValid,
				// model.weights, model.intercept));
			}
		}
	}

	private static void iterateRidge(int[] iterNums, double[] stepSizes, Double regParam, JavaRDD<LabeledPoint> train,
			JavaRDD<LabeledPoint> test) {
		for (int numIter : iterNums) {
			for (double step : stepSizes) {
				RidgeRegressionWithSGD alg = new RidgeRegressionWithSGD();
				alg.setIntercept(true);
				((GradientDescent) alg.optimizer()).setNumIterations(numIter).setRegParam(regParam).setStepSize(step);
				RidgeRegressionModel model = alg.run(train.rdd());
				JavaRDD<Tuple2<Double, Double>> rescaledPredicts = train
						.map((x) -> new Tuple2<Double, Double>(model.predict(x.features()), x.label()));
				JavaRDD<Tuple2<Double, Double>> validPredicts = test
						.map((x) -> new Tuple2<Double, Double>(model.predict(x.features()), x.label()));
				double meanSquared = Math.sqrt(
						rescaledPredicts.mapToDouble((Tuple2<Double, Double> pl) -> Math.pow(pl._1 - pl._2, 2)).mean());
				double meanSquaredValid = Math.sqrt(
						validPredicts.mapToDouble((Tuple2<Double, Double> pl) -> Math.pow(pl._1 - pl._2, 2)).mean());
				System.out.println(
						String.format("%d, %5.3f -> %.4f, %.4f", numIter, step, meanSquared, meanSquaredValid));
			}
		}
	}

	public static void iterateLasso(int[] iterNums, double[] stepSizes, Double regParam, JavaRDD<LabeledPoint> train,
			JavaRDD<LabeledPoint> test) {
		for (int numIter : iterNums) {
			for (double step : stepSizes) {
				LassoWithSGD alg = new LassoWithSGD();
				((GradientDescent) alg.setIntercept(true).optimizer()).setNumIterations(numIter).setStepSize(step)
						.setRegParam(regParam);
				LassoModel model = alg.run(train.rdd());
				JavaRDD<Tuple2<Double, Double>> rescaledPredicts = train
						.map((x) -> new Tuple2<Double, Double>(model.predict(x.features()), x.label()));
				JavaRDD<Tuple2<Double, Double>> validPredicts = test
						.map((x) -> new Tuple2<Double, Double>(model.predict(x.features()), x.label()));
				double meanSquared = Math.sqrt(
						rescaledPredicts.mapToDouble((Tuple2<Double, Double> pl) -> Math.pow(pl._1 - pl._2, 2)).mean());
				double meanSquaredValid = Math.sqrt(
						validPredicts.mapToDouble((Tuple2<Double, Double> pl) -> Math.pow(pl._1 - pl._2, 2)).mean());
				System.out.println(
						String.format("%d, %5.3f -> %.4f, %.4f", numIter, step, meanSquared, meanSquaredValid));
				System.out.println("\tweights: " + model.weights());
			}
		}
	}

	private static void iterateLRwSGDBatch(int[] iterNums, double[] stepSizes, double[] fractions,
			JavaRDD<LabeledPoint> train, JavaRDD<LabeledPoint> test) {
		for (int numIter : iterNums) {
			for (double step : stepSizes) {
				for (double miniBFraction : fractions) {
					LinearRegressionWithSGD alg = new LinearRegressionWithSGD();
					((GradientDescent) alg.setIntercept(true).optimizer()).setNumIterations(numIter).setStepSize(step)
							.setMiniBatchFraction(miniBFraction);
					LinearRegressionModel model = alg.run(train.rdd());
					JavaRDD<Tuple2<Double, Double>> rescaledPredicts = train
							.map((x) -> new Tuple2<Double, Double>(model.predict(x.features()), x.label()));
					JavaRDD<Tuple2<Double, Double>> validPredicts = test
							.map((x) -> new Tuple2<Double, Double>(model.predict(x.features()), x.label()));
					double meanSquared = Math.sqrt(rescaledPredicts
							.mapToDouble((Tuple2<Double, Double> pl) -> Math.pow(pl._1 - pl._2, 2)).mean());
					double meanSquaredValid = Math.sqrt(validPredicts
							.mapToDouble((Tuple2<Double, Double> pl) -> Math.pow(pl._1 - pl._2, 2)).mean());
					System.out.println(String.format("%d, %5.3f %5.3f -> %.4f, %.4f", numIter, step, miniBFraction,
							meanSquared, meanSquaredValid));
				}
			}
		}
	}

	private static void iterateLBFGS(double[] regParams, Integer numCorrections, Double tolerance,
			JavaRDD<LabeledPoint> train, JavaRDD<LabeledPoint> test) {
		int dimnum = train.first().features().size();
		for (double regParam : regParams) {
			Tuple2<Vector, double[]> lbfgsres = LBFGS.runLBFGS(
					train.map((x) -> new Tuple2<Object, Vector>(x.label(), MLUtils.appendBias(x.features()))).rdd(),
					new LeastSquaresGradient(), new SquaredL2Updater(), numCorrections, tolerance, 50000, regParam,
					Vectors.zeros(dimnum + 1));
			Vector weights = lbfgsres._1;
			double[] loss = lbfgsres._2;

			LinearRegressionModel model = new LinearRegressionModel(
					Vectors.dense(Arrays.copyOf(weights.toArray(), weights.size() - 1)),
					weights.apply(weights.size() - 1));

			JavaRDD<Tuple2<Double, Double>> rescaledPredicts = train
					.map((x) -> new Tuple2<Double, Double>(model.predict(x.features()), x.label()));
			JavaRDD<Tuple2<Double, Double>> validPredicts = test
					.map((x) -> new Tuple2<Double, Double>(model.predict(x.features()), x.label()));
			double meanSquared = Math.sqrt(
					rescaledPredicts.mapToDouble((Tuple2<Double, Double> pl) -> Math.pow(pl._1 - pl._2, 2)).mean());
			double meanSquaredValid = Math
					.sqrt(validPredicts.mapToDouble((Tuple2<Double, Double> pl) -> Math.pow(pl._1 - pl._2, 2)).mean());
			System.out.println(
					String.format("%5.3f, %d -> %.4f, %.4f", regParam, numCorrections, meanSquared, meanSquaredValid));
		}
	}

}
