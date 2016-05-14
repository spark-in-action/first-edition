package org.sia;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.base.Optional;

import org.apache.spark.Accumulable;
import org.apache.spark.AccumulableParam;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;
import scala.Tuple4;
import scala.Tuple5;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.bitbucket.dollar.Dollar.*;

public class Chapter4App {

  public static void main(String[] args) throws IOException {

    JavaSparkContext sc = new JavaSparkContext(new SparkConf());

    System.out.println("---- Section 4.1.2 ----");
    JavaRDD<String> tranFile = sc.textFile("first-edition/ch04/ch04_data_transactions.txt");
    JavaRDD<String[]> tranData = tranFile.map((String line) -> line.split("#"));
    JavaPairRDD<Integer, String[]> transByCust = tranData.mapToPair((String[] tran) -> new Tuple2(Integer.valueOf(tran[2]), tran));

    long distinctKeysCount = transByCust.keys().distinct().count();
    System.out.println("Distinct keys count: "+distinctKeysCount);

    Map<Integer, Object> transCountByCust = transByCust.countByKey();

    long sumOfValues = 0;
    for(Object o : transCountByCust.values())
      sumOfValues += ((Long)o).longValue();
    System.out.println();
    System.out.println("Sum of counts by key: "+sumOfValues);

    Map.Entry<Integer, Object> maxPurch = null;

    for(Map.Entry<Integer, Object> entry : transCountByCust.entrySet())
    {
      if(maxPurch == null || (Long)entry.getValue() > (Long)maxPurch.getValue())
        maxPurch = entry;
    }
    System.out.println();
    System.out.println("Max purchases ("+maxPurch.getValue()+") were made by "+maxPurch.getKey());

    List<String[]> complTrans = new ArrayList<String[]>();
    String[] newTrans = {"2015-03-30", "11:59 PM", "53", "4", "1", "0.00"};
    complTrans.add(newTrans);

    List<String[]> cust53Trans = transByCust.lookup(53);
    System.out.println();
    System.out.println("Customer 53's transactions: ");
    for(String[] tran : cust53Trans)
      System.out.println(String.join(", ", tran));

    transByCust = transByCust.mapValues((String[] tran) -> {
      if(Integer.parseInt(tran[3]) == 25 && Double.parseDouble(tran[4]) > 1)
        tran[5] = String.valueOf(Double.parseDouble(tran[5]) * 0.95);
      return tran;
    });

    transByCust = transByCust.flatMapValues((String[] tran) -> {
      ArrayList<String[]> newlist = new ArrayList<String[]>();
      newlist.add(tran);
      if(Integer.parseInt(tran[3]) == 81 && Integer.parseInt(tran[4]) >= 5) {
        String[] cloned = Arrays.copyOf(tran, tran.length);
        cloned[5] = "0.00"; cloned[3] = "70"; cloned[4] = "1";
        newlist.add(cloned);
        }
      return newlist;
    });

    System.out.println();
    System.out.println("TransByCust new count: "+transByCust.count());

    JavaPairRDD<Integer, Double> amounts = transByCust.mapValues((String[] t) -> Double.parseDouble(t[5]));
    List<Tuple2<Integer, Double>> totals = amounts.foldByKey(new Double(0), (Double p1, Double p2) -> p1 + p2).collect();
    System.out.println();
    System.out.println("Totals with 0 zero value: "+totals);

    Tuple2<Integer, Double> maxAmount = null;
    for(Tuple2<Integer, Double> am : totals) {
      if(maxAmount == null || am._2 > maxAmount._2)
        maxAmount = am;
    }
    System.out.println();
    System.out.println("Customer "+maxAmount._1+" spent "+maxAmount._2);

    List<Tuple2<Integer, Double>> totals2 = amounts.foldByKey(new Double(100000), (Double p1, Double p2) -> p1 + p2).collect();
    System.out.println();
    System.out.println("Totals with 100000 zero value: "+totals2);

    String[] newTrans2 = {"2015-03-30", "11:59 PM", "76", "63", "1", "0.00"};
    complTrans.add(newTrans2);
    transByCust = transByCust.union(sc.parallelize(complTrans).mapToPair((String[] t) -> new Tuple2(Integer.valueOf(t[2]), t)));
    transByCust.map((Tuple2<Integer, String[]> t) -> String.join("#", t._2)).saveAsTextFile("ch04output-transByCust");

    JavaPairRDD<Integer, List<String>> prods = transByCust.aggregateByKey(new ArrayList<String>(),
       (List<String> prods2, String[] tran) -> { prods2.add(tran[3]); return prods2; },
       (List<String> prods1, List<String> prods2) -> { prods1.addAll(prods2); return prods1; });
    System.out.println();
    System.out.println("Products per customer: "+prods.collect());

    System.out.println("\n\n---- Section 4.2.2 ----");
    JavaRDD<Integer> rdd = sc.parallelize($(1, 10000).toList());
    System.out.println();
    System.out.println("Map without a shuffle: "+rdd.mapToPair((Integer x) -> new Tuple2<Integer, Integer>(x, x*x)).map((Tuple2<Integer, Integer> t) -> t.swap()).count());
    System.out.println("Map with a shuffle: "+rdd.mapToPair((Integer x) -> new Tuple2<Integer, Integer>(x, x*x)).reduceByKey((Integer v1, Integer v2) -> v1+v2).count());

    System.out.println("\n\n---- Section 4.2.4 ----");
    List<Integer> list = $(new Random(), 100).samples(500).toList();//  List.fill(500)(scala.util.Random.nextInt(100));
    JavaRDD<List<Integer>> rdd424 = sc.parallelize(list, 30).glom();
    System.out.println();
    System.out.println("Glommed RDD: "+rdd424.collect());
    System.out.println("Glommed RDD count: "+rdd424.count());

    System.out.println("\n\n---- Section 4.3.1 ----");
    JavaPairRDD<Integer, String[]> transByProd = tranData.mapToPair((String[] tran) -> new Tuple2(Integer.valueOf(tran[3]), tran));
    JavaPairRDD<Integer, Double> totalsByProd = transByProd.mapValues(t -> Double.valueOf(t[5])).
       reduceByKey((Double tot1, Double tot2) -> tot1 + tot2);

    JavaPairRDD<Integer, String[]> products = sc.textFile("first-edition/ch04/ch04_data_products.txt").
        map((String line) -> line.split("#")).
        mapToPair((String[] p) -> new Tuple2(Integer.valueOf(p[0]), p));
    JavaPairRDD<Integer, Tuple2<Double, String[]>> totalsAndProds = totalsByProd.join(products);
    System.out.println();
    System.out.println("First joined product: "+totalsAndProds.first());

    JavaPairRDD<Integer, Tuple2<Optional<Double>, String[]>> totalsWithMissingProds = totalsByProd.rightOuterJoin(products);
    JavaRDD<String[]> missingProds = totalsWithMissingProds.filter((Tuple2<Integer, Tuple2<Optional<Double>, String[]>> x) -> !x._2._1.isPresent()).
        map((Tuple2<Integer, Tuple2<Optional<Double>, String[]>> x) -> x._2._2);
    System.out.println();
    System.out.println("Missing products:");
    missingProds.foreach((String[] p) -> System.out.println(String.join(", ", p)));

    JavaRDD<String[]> missingProdsSubtr = products.subtractByKey(totalsByProd).values();
    System.out.println();
    System.out.println("Missing products with subtract:");
    missingProdsSubtr.foreach((String[] p) -> System.out.println(String.join(", ", p)));

    JavaPairRDD<Integer, Tuple2<Iterable<Double>, Iterable<String[]>>> prodTotCogroup = totalsByProd.cogroup(products);
    System.out.println();
    System.out.println("Missing products with cogroup:");
    prodTotCogroup.filter((Tuple2<Integer, Tuple2<Iterable<Double>, Iterable<String[]>>> x) -> !x._2._1.iterator().hasNext()).
      foreach((Tuple2<Integer, Tuple2<Iterable<Double>, Iterable<String[]>>> x) -> System.out.println(String.join(", ", x._2._2.iterator().next())));
    JavaPairRDD<Integer, Tuple2<Double, String[]>> totalsAndProds2 = prodTotCogroup.filter((Tuple2<Integer, Tuple2<Iterable<Double>, Iterable<String[]>>> x) -> x._2._1.iterator().hasNext()).
      mapToPair((Tuple2<Integer, Tuple2<Iterable<Double>, Iterable<String[]>>> x) -> new Tuple2(Integer.valueOf(x._2._2.iterator().next()[0]), new Tuple2(x._2._1.iterator().next(), x._2._2.iterator().next())));
    System.out.println();
    System.out.println("First joined product from totalsAndProds2: "+totalsAndProds2.first());

    totalsByProd.map((Tuple2<Integer, Double> x) -> x._1).intersection(products.map((Tuple2<Integer, String[]> x) -> x._1));

    JavaRDD<Integer> rdd1 = sc.parallelize($(7,9).toList());
    JavaRDD<Integer> rdd2 = sc.parallelize($(1,3).toList());
    System.out.println();
    System.out.println("Cartesian: "+rdd1.cartesian(rdd2).collect());
    System.out.println("Only divisible: "+rdd1.cartesian(rdd2).filter((Tuple2<Integer, Integer> el) -> el._1 % el._2 == 0).collect());

    JavaRDD<Integer> rdd3 = sc.parallelize($(1,4).toList());
    JavaRDD<String> rdd4 = sc.parallelize($("n4 n5 n6".split(" ")).toList());
    System.out.println();
    System.out.println(rdd3.zip(rdd4).collect());

    JavaRDD<Integer> rdd5 = sc.parallelize($(1, 11).toList(), 10);
    JavaRDD<String> rdd6 = sc.parallelize($("n1 n2 n3 n4 n5 n6 n7 n8".split(" ")).toList(), 10);
    System.out.println();
    System.out.println("zipPartitions result: "+
      rdd5.zipPartitions(rdd6, (Iterator<Integer> iter1, Iterator<String> iter2) -> {
        List<String> retlist = new ArrayList<String>();
        while(iter1.hasNext() || iter2.hasNext())
        {
          int val1 = -1;
          String val2 = "empty";
          if(iter1.hasNext())
            val1 = iter1.next();
          if(iter2.hasNext())
            val2 = iter2.next();
          retlist.add(val1+"-"+val2);
          }
        return retlist;
      }).collect());

    System.out.println("\n\n---- Section 4.3.2 ----");
    JavaPairRDD<Integer, Tuple2<Double, String[]>> sortedProds = totalsAndProds.mapToPair((Tuple2<Integer, Tuple2<Double, String[]>> t) -> new Tuple2(t._2._2[1], t)).
    sortByKey().
    mapToPair((Tuple2<Object, Object> t) -> (Tuple2<Integer, Tuple2<Double, String[]>>)t._2);
    System.out.println();
    System.out.println("Products sorted alphabetically: "+sortedProds.collect());

    System.out.println("\n\n---- Section 4.3.3 ----");
    Function<String[], Tuple4<Double, Double, Integer, Double>> createComb = (t) -> {
      Double total = Double.valueOf(t[5]);
      Integer q = Integer.valueOf(t[4]);
      return new Tuple4(total/q, total/q, q, total);
    };
    Function2<Tuple4<Double, Double, Integer, Double>, String[], Tuple4<Double, Double, Integer, Double>> mergeVal = (tup, t) -> {
        Double total = Double.valueOf(t[5]);
        Integer q = Integer.valueOf(t[4]);
        return new Tuple4(Math.min(tup._1(),total/q),Math.max(tup._2(),total/q),tup._3()+q,tup._4()+total);
    };
    Function2<Tuple4<Double, Double, Integer, Double>, Tuple4<Double, Double, Integer, Double>, Tuple4<Double, Double, Integer, Double>> mergeComb = (tup1, tup2) -> {
      return new Tuple4(Math.min(tup1._1(),tup2._1()),Math.max(tup1._2(),tup2._2()),tup1._3()+tup2._3(),tup1._4()+tup2._4());
    };
    JavaPairRDD<Integer, Tuple5<Double, Double, Integer, Double, Double>> avgByCust = transByCust.combineByKey(createComb, mergeVal, mergeComb,
             new org.apache.spark.HashPartitioner(transByCust.partitions().size())).
             mapValues((Tuple4<Double, Double, Integer, Double> t) -> new Tuple5(t._1(),t._2(),t._3(),t._4(),t._4()/t._3()));
    System.out.println();
    System.out.println("First element of avgByCust: "+avgByCust.first());

    totalsAndProds.map((Tuple2<Integer, Tuple2<Double, String[]>> t) -> t._2).
      map((Tuple2<Double, String[]> x) -> String.join("#", x._2)+", "+x._1).saveAsTextFile("ch04output-totalsPerProd");
    avgByCust.map((Tuple2<Integer, Tuple5<Double, Double, Integer, Double, Double>> t) -> t._1+"#"+t._2._1()+"#"+t._2._2()+"#"+t._2._3()+"#"+t._2._4()+"#"+t._2._5()).saveAsTextFile("ch04output-avgByCust");

    System.out.println("\n\n---- Section 4.4.1 ----");
    List<Integer> randList = $(new Random(), 10).samples(500).toList();
    JavaRDD<Integer> listrdd = sc.parallelize(randList, 5);
    JavaPairRDD<Integer, Integer> pairs = listrdd.mapToPair((Integer x) -> new Tuple2(x, x*x));
    JavaPairRDD<Integer, Integer> reduced = pairs.reduceByKey((Integer v1, Integer v2) -> v1+v2);
    JavaRDD<String> finalrdd = reduced.mapPartitions((Iterator<Tuple2<Integer, Integer>> iter) -> {
      List<String> res = new ArrayList();
      while(iter.hasNext())
      {
        Tuple2 t = iter.next();
        res.add("K="+t._1+",V="+t._2);
      }
      return res;
    });
    System.out.println();
    System.out.println("MapPartitions results: "+finalrdd.collect());
    System.out.println("finalrdd.toDebugString: \n"+finalrdd.toDebugString());

    System.out.println("\n\n---- Section 4.5.1 ----");
    Accumulator<Integer> acc = sc.accumulator(0, "acc name");
    JavaRDD<Integer> rlist = sc.parallelize($(1, 1000000).toList());
    rlist.foreach((Integer x) -> acc.add(1));
    System.out.println();
    System.out.println("Accumulator value: "+acc.value());
    //throws exception:
    //list.foreach((Integer x) -> acc.value);

    JavaRDD<Integer> accrdd = sc.parallelize($(1, 100).toList());

    Accumulable<Tuple2<Integer, Integer>, Integer> accumulable = sc.accumulable(new Tuple2(0,0), new AvgAccParam());
    accrdd.foreach((Integer x) -> accumulable.add(x));
    System.out.println();
    System.out.println("Mean value from accumulable: "+Double.valueOf(accumulable.value()._2) / accumulable.value()._1);
  }
  private static class AvgAccParam implements AccumulableParam<Tuple2<Integer, Integer>, Integer> {
  @Override
  public Tuple2<Integer, Integer> addAccumulator(Tuple2<Integer, Integer> v1, Integer v2) {
    return new Tuple2<Integer, Integer>(v1._1+1, v1._2+v2);
  }

  @Override
  public Tuple2<Integer, Integer> addInPlace(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) {
    return new Tuple2<Integer, Integer>(v1._1+v2._1, v1._2+v2._2);
  }

  @Override
  public Tuple2<Integer, Integer> zero(Tuple2<Integer, Integer> v) {
    return new Tuple2<Integer, Integer>(0, 0);
  }
  }

}
