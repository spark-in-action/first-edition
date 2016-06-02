
//section 4.1.2
val tranFile = sc.textFile("first-edition/ch04/ch04_data_transactions.txt")
val tranData = tranFile.map(_.split("#"))
var transByCust = tranData.map(tran => (tran(2).toInt, tran))

transByCust.keys.distinct().count()

transByCust.countByKey()
transByCust.countByKey().values.sum
val (cid, purch) = transByCust.countByKey().toSeq.sortBy(_._2).last
var complTrans = Array(Array("2015-03-30", "11:59 PM", "53", "4", "1", "0.00"))

transByCust.lookup(53)
transByCust.lookup(53).foreach(tran => println(tran.mkString(", ")))

transByCust = transByCust.mapValues(tran => {
     if(tran(3).toInt == 25 && tran(4).toDouble > 1)
         tran(5) = (tran(5).toDouble * 0.95).toString
     tran })

transByCust = transByCust.flatMapValues(tran => {
    if(tran(3).toInt == 81 && tran(4).toInt >= 5) {
       val cloned = tran.clone()
       cloned(5) = "0.00"; cloned(3) = "70"; cloned(4) = "1";
       List(tran, cloned)
    }
    else
       List(tran)
    })


val amounts = transByCust.mapValues(t => t(5).toDouble)
val totals = amounts.foldByKey(0)((p1, p2) => p1 + p2).collect()
totals.toSeq.sortBy(_._2).last
amounts.foldByKey(100000)((p1, p2) => p1 + p2).collect()

complTrans = complTrans :+ Array("2015-03-30", "11:59 PM", "76", "63", "1", "0.00")
transByCust = transByCust.union(sc.parallelize(complTrans).map(t => (t(2).toInt, t)))
transByCust.map(t => t._2.mkString("#")).saveAsTextFile("ch04output-transByCust")

val prods = transByCust.aggregateByKey(List[String]())(
   (prods, tran) => prods ::: List(tran(3)),
   (prods1, prods2) => prods1 ::: prods2)
prods.collect()

//section 4.2.2
import org.apache.spark.rdd.RDD
val rdd:RDD[Int] = sc.parallelize(1 to 10000)
rdd.map(x => (x, x*x)).map(_.swap).collect()
rdd.map(x => (x, x*x)).reduceByKey((v1, v2)=>v1+v2).collect()

//section 4.2.4
val list = List.fill(500)(scala.util.Random.nextInt(100))
val rdd = sc.parallelize(list, 30).glom()
rdd.collect()
rdd.count()

//section 4.3.1
val transByProd = tranData.map(tran => (tran(3).toInt, tran))
val totalsByProd = transByProd.mapValues(t => t(5).toDouble).
   reduceByKey{case(tot1, tot2) => tot1 + tot2}

val products = sc.textFile("first-edition/ch04/ch04_data_products.txt").
    map(line => line.split("#")).
    map(p => (p(0).toInt, p))
val totalsAndProds = totalsByProd.join(products)
totalsAndProds.first()

val totalsWithMissingProds = totalsByProd.rightOuterJoin(products)
val missingProds = totalsWithMissingProds.filter(x => x._2._1 == None).map(x => x._2._2)
missingProds.foreach(p => println(p.mkString(", ")))


val missingProds = products.subtractByKey(totalsByProd).values
missingProds.foreach(p => println(p.mkString(", ")))

val prodTotCogroup = totalsByProd.cogroup(products)
prodTotCogroup.filter(x => x._2._1.isEmpty).
  foreach(x => println(x._2._2.head.mkString(", ")))
val totalsAndProds = prodTotCogroup.filter(x => !x._2._1.isEmpty).
  map(x => (x._2._2.head(0).toInt,(x._2._1.head, x._2._2.head)))

totalsByProd.map(_._1).intersection(products.map(_._1))

val rdd1 = sc.parallelize(List(7,8,9))
val rdd2 = sc.parallelize(List(1,2,3))
rdd1.cartesian(rdd2).collect()
rdd1.cartesian(rdd2).filter(el => el._1 % el._2 == 0).collect()

val rdd1 = sc.parallelize(List(1,2,3))
val rdd2 = sc.parallelize(List("n4","n5","n6"))
rdd1.zip(rdd2).collect()

val rdd1 = sc.parallelize(1 to 10, 10)
val rdd2 = sc.parallelize((1 to 8).map(x=>"n"+x), 10)
rdd1.zipPartitions(rdd2, true)((iter1, iter2) => {
        iter1.zipAll(iter2, -1, "empty")
        .map({case(x1, x2)=>x1+"-"+x2})
    }).collect()

//Section 4.3.2
val sortedProds = totalsAndProds.sortBy(_._2._2(1))
sortedProds.collect()

case class Employee(lastName: String) extends Ordered[Employee] {
    override def compare(that: Employee) =
        this.lastName.compare(that.lastName)
}

implicit val emplOrdering = new Ordering[Employee] {
    override def compare(a: Employee, b: Employee) = a.lastName.compare(b.lastName)
}
implicit val emplOrdering: Ordering[Employee] = Ordering.by(_.lastName)

//Section 4.3.3
def createComb = (t:Array[String]) => {
  val total = t(5).toDouble
  val q = t(4).toInt
  (total/q, total/q, q, total) }
def mergeVal:((Double,Double,Int,Double),Array[String])=>(Double,Double,Int,Double) =
    { case((mn,mx,c,tot),t) => {
      val total = t(5).toDouble
      val q = t(4).toInt
      (scala.math.min(mn,total/q),scala.math.max(mx,total/q),c+q,tot+total) } }
def mergeComb:((Double,Double,Int,Double),(Double,Double,Int,Double))=>(Double,Double,Int,Double) =
         {case((mn1,mx1,c1,tot1),(mn2,mx2,c2,tot2)) =>
         (scala.math.min(mn1,mn2),scala.math.max(mx1,mx2),c1+c2,tot1+tot2) }
val avgByCust = transByCust.combineByKey(createComb, mergeVal, mergeComb,
         new org.apache.spark.HashPartitioner(transByCust.partitions.size)).
         mapValues({case(mn,mx,cnt,tot) => (mn,mx,cnt,tot,tot/cnt)})
avgByCust.first()

totalsAndProds.map(_._2).map(x=>x._2.mkString("#")+", "+x._1).saveAsTextFile("ch04output-totalsPerProd")
avgByCust.map{ case (id, (min, max, cnt, tot, avg)) => "%d#%.2f#%.2f#%d#%.2f#%.2f".format(id, min, max, cnt, tot, avg)}.saveAsTextFile("ch04output-avgByCust")

//Section 4.4.1
val list = List.fill(500)(scala.util.Random.nextInt(10))
val listrdd = sc.parallelize(list, 5)
val pairs = listrdd.map(x => (x, x*x))
val reduced = pairs.reduceByKey((v1, v2)=>v1+v2)
val finalrdd = reduced.mapPartitions(iter => iter.map({case(k,v)=>"K="+k+",V="+v}))
finalrdd.collect()
println(finalrdd.toDebugString)

//Section 4.5.1
val acc = sc.accumulator(0, "acc name")
val list = sc.parallelize(1 to 1000000)
list.foreach(x => acc.add(1))
acc.value
list.foreach(x => acc.value)

val rdd = sc.parallelize(1 to 100)
import org.apache.spark.AccumulableParam
implicit object AvgAccParam extends AccumulableParam[(Int, Int), Int] {
  def zero(v:(Int, Int)) = (0, 0)
  def addInPlace(v1:(Int, Int), v2:(Int, Int)) = (v1._1+v2._1, v1._2+v2._2)
  def addAccumulator(v1:(Int, Int), v2:Int) = (v1._1+1, v1._2+v2)
}
val acc = sc.accumulable((0,0))
rdd.foreach(x => acc += x)
val mean = acc.value._2.toDouble / acc.value._1

import scala.collection.mutable.MutableList
val colacc = sc.accumulableCollection(MutableList[Int]())
rdd.foreach(x => colacc += x)
colacc.value
