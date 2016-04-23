package org.sia.chapter03App

import scala.io.Source.fromFile
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object GitHubDay {
  def main(args : Array[String]) {
    val sc = new SparkContext(new SparkConf())
    val sqlContext = new SQLContext(sc)

    val ghLog = sqlContext.read.json(args(0))

    val pushes = ghLog.filter("type = 'PushEvent'")
    val grouped = pushes.groupBy("actor.login").count
    val ordered = grouped.orderBy(grouped("count").desc)

    // Broadcast the employees set
    val employees = Set() ++ (
      for {
        line <- fromFile(args(1)).getLines
      } yield line.trim
    )
    val bcEmployees = sc.broadcast(employees)

    import sqlContext.implicits._
    val isEmp = user => bcEmployees.value.contains(user)
    val sqlFunc = sqlContext.udf.register("SetContainsUdf", isEmp)
    val filtered = ordered.filter(sqlFunc($"login"))

    filtered.write.format(args(3)).save(args(2))
  }
}
