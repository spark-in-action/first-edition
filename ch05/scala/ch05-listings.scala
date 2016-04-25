
// Section 5.1.1

import sqlContext.implicits._

val itPostsRows = sc.textFile("first-edition/ch05/italianPosts.csv")
val itPostsSplit = itPostsRows.map(x => x.split("~"))

val itPostsRDD = itPostsSplit.map(x => (x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12)))
val itPostsDFrame = itPostsRDD.toDF()
itPostsDFrame.show(10)
// +---+--------------------+---+--------------------+---+--------------------+----+--------------------+--------------------+----+----+---+----+
// | _1|                  _2| _3|                  _4| _5|                  _6|  _7|                  _8|                  _9| _10| _11|_12| _13|
// +---+--------------------+---+--------------------+---+--------------------+----+--------------------+--------------------+----+----+---+----+
// |  4|2013-11-11 18:21:...| 17|&lt;p&gt;The infi...| 23|2013-11-10 19:37:...|null|                    |                    |null|null|  2|1165|
// |  5|2013-11-10 20:31:...| 12|&lt;p&gt;Come cre...|  1|2013-11-10 19:44:...|  61|Cosa sapreste dir...| &lt;word-choice&gt;|   1|null|  1|1166|
// |  2|2013-11-10 20:31:...| 17|&lt;p&gt;Il verbo...|  5|2013-11-10 19:58:...|null|                    |                    |null|null|  2|1167|
// |  1|2014-07-25 13:15:...|154|&lt;p&gt;As part ...| 11|2013-11-10 22:03:...| 187|Ironic constructi...|&lt;english-compa...|   4|1170|  1|1168|
// |  0|2013-11-10 22:15:...| 70|&lt;p&gt;&lt;em&g...|  3|2013-11-10 22:15:...|null|                    |                    |null|null|  2|1169|
// |  2|2013-11-10 22:17:...| 17|&lt;p&gt;There's ...|  8|2013-11-10 22:17:...|null|                    |                    |null|null|  2|1170|
// |  1|2013-11-11 09:51:...| 63|&lt;p&gt;As other...|  3|2013-11-11 09:51:...|null|                    |                    |null|null|  2|1171|
// |  1|2013-11-12 23:57:...| 63|&lt;p&gt;The expr...|  1|2013-11-11 10:09:...|null|                    |                    |null|null|  2|1172|
// |  9|2014-01-05 11:13:...| 63|&lt;p&gt;When I w...|  5|2013-11-11 10:28:...| 122|Is &quot;scancell...|&lt;usage&gt;&lt;...|   3|1181|  1|1173|
// |  0|2013-11-11 10:58:...| 18|&lt;p&gt;Wow, wha...|  5|2013-11-11 10:58:...|null|                    |                    |null|null|  2|1174|
// +---+--------------------+---+--------------------+---+--------------------+----+--------------------+--------------------+----+----+---+----+
// only showing top 10 rows

val itPostsDF = itPostsRDD.toDF("commentCount", "lastActivityDate", "ownerUserId", "body", "score", "creationDate", "viewCount", "title", "tags", "answerCount", "acceptedAnswerId", "postTypeId", "id")

itPostsDF.printSchema
// root
//  |-- commentCount: string (nullable = true)
//  |-- lastActivityDate: string (nullable = true)
//  |-- ownerUserId: string (nullable = true)
//  |-- body: string (nullable = true)
//  |-- score: string (nullable = true)
//  |-- creationDate: string (nullable = true)
//  |-- viewCount: string (nullable = true)
//  |-- title: string (nullable = true)
//  |-- tags: string (nullable = true)
//  |-- answerCount: string (nullable = true)
//  |-- acceptedAnswerId: string (nullable = true)
//  |-- postTypeId: string (nullable = true)
//  |-- id: string (nullable = true)

import java.sql.Timestamp
case class Post (commentCount:Option[Int], lastActivityDate:Option[Timestamp],
  ownerUserId:Option[Long], body:String, score:Option[Int], creationDate:Option[Timestamp],
  viewCount:Option[Int], title:String, tags:String, answerCount:Option[Int],
  acceptedAnswerId:Option[Long], postTypeId:Option[Long], id:Long)

object StringImplicits {
   implicit class StringImprovements(val s: String) {
      import scala.util.control.Exception.catching
      def toIntSafe = catching(classOf[NumberFormatException]) opt s.toInt
      def toLongSafe = catching(classOf[NumberFormatException]) opt s.toLong
      def toTimestampSafe = catching(classOf[IllegalArgumentException]) opt Timestamp.valueOf(s)
   }
}

import StringImplicits._
def stringToPost(row:String):Post = {
  val r = row.split("~")
  Post(r(0).toIntSafe,
    r(1).toTimestampSafe,
    r(2).toLongSafe,
    r(3),
    r(4).toIntSafe,
    r(5).toTimestampSafe,
    r(6).toIntSafe,
    r(7),
    r(8),
    r(9).toIntSafe,
    r(10).toLongSafe,
    r(11).toLongSafe,
    r(12).toLong)
}
val itPostsDFCase = itPostsRows.map(x => stringToPost(x)).toDF()
itPostsDFCase.printSchema
// root
//  |-- commentCount: integer (nullable = true)
//  |-- lastActivityDate: timestamp (nullable = true)
//  |-- ownerUserId: long (nullable = true)
//  |-- body: string (nullable = true)
//  |-- score: integer (nullable = true)
//  |-- creationDate: timestamp (nullable = true)
//  |-- viewCount: integer (nullable = true)
//  |-- title: string (nullable = true)
//  |-- tags: string (nullable = true)
//  |-- answerCount: integer (nullable = true)
//  |-- acceptedAnswerId: long (nullable = true)
//  |-- postTypeId: long (nullable = true)
//  |-- id: long (nullable = false)

import org.apache.spark.sql.types._
val postSchema = StructType(Seq(
  StructField("commentCount", IntegerType, true),
  StructField("lastActivityDate", TimestampType, true),
  StructField("ownerUserId", LongType, true),
  StructField("body", StringType, true),
  StructField("score", IntegerType, true),
  StructField("creationDate", TimestampType, true),
  StructField("viewCount", IntegerType, true),
  StructField("title", StringType, true),
  StructField("tags", StringType, true),
  StructField("answerCount", IntegerType, true),
  StructField("acceptedAnswerId", LongType, true),
  StructField("postTypeId", LongType, true),
  StructField("id", LongType, false))
  )
import org.apache.spark.sql.Row
def stringToRow(row:String):Row = {
  val r = row.split("~")
  Row(r(0).toIntSafe,
    r(1).toTimestampSafe,
    r(2).toLongSafe,
    r(3),
    r(4).toIntSafe,
    r(5).toTimestampSafe,
    r(6).toIntSafe,
    r(7),
    r(8),
    r(9).toIntSafe,
    r(10).toLongSafe,
    r(11).toLongSafe,
    r(12).toLong)
}
val rowRDD = itPostsRows.map(row => stringToRow(row))
val itPostsDFStruct = sqlContext.createDataFrame(rowRDD, postSchema)
itPostsDFStruct.columns
itPostsDFStruct.dtypes

//Section 5.1.2

val postsDf = itPostsDFStruct
val postsIdBody = postsDf.select("id", "body")

val postsIdBody = postsDf.select(postsDf.col("id"), postsDf.col("body"))
val postsIdBody = postsDf.select(Symbol("id"), Symbol("body"))
val postsIdBody = postsDf.select('id, 'body)
val postsIdBody = postsDf.select($"id", $"body")

val postIds = postsIdBody.drop("body")

postsIdBody.filter('body contains "Italiano").count()

val noAnswer = postsDf.filter(('postTypeId === 1) and ('acceptedAnswerId isNull))

val firstTenQs = postsDf.filter('postTypeId === 1).limit(10)
val firstTenQsRn = firstTenQs.withColumnRenamed("ownerUserId", "owner")

postsDf.filter('postTypeId === 1).withColumn("ratio", 'viewCount / 'score).where('ratio < 35).show()
// +------------+--------------------+-----------+--------------------+-----+--------------------+---------+--------------------+--------------------+-----------+----------------+----------+----+-------------------+
// |commentCount|    lastActivityDate|ownerUserId|                body|score|        creationDate|viewCount|               title|                tags|answerCount|acceptedAnswerId|postTypeId|  id|              ratio|
// +------------+--------------------+-----------+--------------------+-----+--------------------+---------+--------------------+--------------------+-----------+----------------+----------+----+-------------------+
// |           1|2014-07-25 13:15:...|        154|&lt;p&gt;As part ...|   11|2013-11-10 22:03:...|      187|Ironic constructi...|&lt;english-compa...|          4|            1170|         1|1168|               17.0|
// |           9|2014-01-05 11:13:...|         63|&lt;p&gt;When I w...|    5|2013-11-11 10:28:...|      122|Is &quot;scancell...|&lt;usage&gt;&lt;...|          3|            1181|         1|1173|               24.4|
// |           1|2014-01-16 19:56:...|         63|&lt;p&gt;Suppose ...|    4|2013-11-11 11:31:...|      114|How should I tran...|&lt;usage&gt;&lt;...|          2|            1177|         1|1175|               28.5|
// |           0|2013-11-11 14:36:...|         63|&lt;p&gt;Except w...|    3|2013-11-11 11:39:...|       58|Using a comma bet...|&lt;usage&gt;&lt;...|          2|            1182|         1|1176| 19.333333333333332|
// |           0|2013-11-12 11:24:...|         63|&lt;p&gt;Comparin...|    3|2013-11-11 12:58:...|       60|Using the conditi...|&lt;usage&gt;&lt;...|          2|            1180|         1|1179|               20.0|
// |           2|2013-11-11 23:23:...|        159|&lt;p&gt;Sono un'...|    7|2013-11-11 18:19:...|      138|origine dell'espr...|&lt;idioms&gt;&lt...|          2|            1185|         1|1184| 19.714285714285715|
// |           5|2013-11-21 14:04:...|          8|&lt;p&gt;The use ...|   13|2013-11-11 21:01:...|      142|Usage of preposit...|&lt;prepositions&...|          2|            1212|         1|1192| 10.923076923076923|
// |           0|2013-11-12 09:26:...|         17|&lt;p&gt;When wri...|    5|2013-11-11 21:01:...|       70|What's the correc...|&lt;punctuation&g...|          4|            1195|         1|1193|               14.0|
// |           1|2013-11-24 09:35:...|         12|&lt;p&gt;Are &quo...|    8|2013-11-11 21:44:...|      135|Are &quot;qui&quo...|     &lt;grammar&gt;|          2|            null|         1|1197|             16.875|
// |           0|2013-11-12 10:32:...|         63|&lt;p&gt;When wri...|    1|2013-11-12 09:31:...|       34|Period after abbr...|&lt;usage&gt;&lt;...|          1|            1202|         1|1201|               34.0|
// |           1|2013-11-12 12:53:...|         99|&lt;p&gt;I can't ...|   -3|2013-11-12 10:57:...|       68|Past participle o...|&lt;grammar&gt;&l...|          3|            1216|         1|1203|-22.666666666666668|
// |           4|2014-09-09 08:54:...|         63|&lt;p&gt;Some wor...|    4|2013-11-12 11:03:...|       80|When is the lette...|&lt;nouns&gt;&lt;...|          2|            1207|         1|1205|               20.0|
// |           1|2013-11-13 18:45:...|         12|&lt;p&gt;Sapreste...|    4|2013-11-12 13:10:...|       82|Quali sono gli er...|     &lt;grammar&gt;|          2|            null|         1|1217|               20.5|
// |           6|2014-04-09 18:02:...|         63|&lt;p&gt;In Lomba...|    5|2013-11-12 13:12:...|      100|Is &quot;essere d...|&lt;usage&gt;&lt;...|          4|            1226|         1|1218|               20.0|
// |           3|2014-09-11 14:37:...|         63|&lt;p&gt;The plur...|    5|2013-11-12 13:34:...|       59|Why is the plural...|&lt;plural&gt;&lt...|          1|            1227|         1|1221|               11.8|
// |           1|2013-11-12 13:49:...|         63|&lt;p&gt;I rememb...|    6|2013-11-12 13:38:...|       53|Can &quot;sciĂ˛&qu...|&lt;usage&gt;&lt;...|          1|            1223|         1|1222|  8.833333333333334|
// |           2|2014-08-14 17:11:...|         99|&lt;p&gt;Is it ma...|    7|2013-11-12 14:11:...|      163|Usage of accented...|&lt;word-choice&g...|          2|            1228|         1|1224| 23.285714285714285|
// |           2|2013-11-14 11:50:...|         18|&lt;p&gt;Where do...|    5|2013-11-12 14:49:...|      123|Etymology of &quo...|&lt;idioms&gt;&lt...|          1|            1230|         1|1229|               24.6|
// |           0|2013-11-12 18:50:...|         53|&lt;p&gt;Do all I...|    4|2013-11-12 15:38:...|       62|Do adjectives alw...|&lt;adjectives&gt...|          2|            1234|         1|1231|               15.5|
// |           5|2014-04-26 17:30:...|        132|&lt;p&gt;In Itali...|    7|2013-11-12 20:01:...|      230|&quot;Darsi del t...|&lt;usage&gt;&lt;...|          4|            null|         1|1238| 32.857142857142854|
// +------------+--------------------+-----------+--------------------+-----+--------------------+---------+--------------------+--------------------+-----------+----------------+----------+----+-------------------+
// only showing top 20 rows

//The 10 most recently modified questions:
postsDf.filter('postTypeId === 1).orderBy('lastActivityDate desc).limit(10).show

//Section 5.1.3

import org.apache.spark.sql.functions._
postsDf.filter('postTypeId === 1).withColumn("activePeriod", datediff('lastActivityDate, 'creationDate)).orderBy('activePeriod desc).head.getString(3).replace("&lt;","<").replace("&gt;",">")
//res0: String = <p>The plural of <em>braccio</em> is <em>braccia</em>, and the plural of <em>avambraccio</em> is <em>avambracci</em>.</p><p>Why are the plural of those words so different, if they both are referring to parts of the human body, and <em>avambraccio</em> derives from <em>braccio</em>?</p>

postsDf.select(avg('score), max('score), count('score)).show


import org.apache.spark.sql.expressions.Window
postsDf.filter('postTypeId === 1).select('ownerUserId, 'acceptedAnswerId, score, max('score).over(Window.partitionBy('ownerUserId)) as "maxPerUser").withColumn("toMax", 'maxPerUser - 'score).show(10)
// +-----------+----------------+-----+----------+-----+
// |ownerUserId|acceptedAnswerId|score|maxPerUser|toMax|
// +-----------+----------------+-----+----------+-----+
// |        232|            2185|    6|         6|    0|
// |        833|            2277|    4|         4|    0|
// |        833|            null|    1|         4|    3|
// |        235|            2004|   10|        10|    0|
// |        835|            2280|    3|         3|    0|
// |         37|            null|    4|        13|    9|
// |         37|            null|   13|        13|    0|
// |         37|            2313|    8|        13|    5|
// |         37|              20|   13|        13|    0|
// |         37|            null|    4|        13|    9|
// +-----------+----------------+-----+----------+-----+

postsDf.filter('postTypeId === 1).select('ownerUserId, 'id, 'creationDate, lag('id, 1).over(Window.partitionBy('ownerUserId).orderBy('creationDate)) as "prev", lead('id, 1).over(Window.partitionBy('ownerUserId).orderBy('creationDate)) as "next").orderBy('ownerUserId, 'id).show()
// +-----------+----+--------------------+----+----+
// |ownerUserId|  id|        creationDate|prev|next|
// +-----------+----+--------------------+----+----+
// |          4|1637|2014-01-24 06:51:...|null|null|
// |          8|   1|2013-11-05 20:22:...|null| 112|
// |          8| 112|2013-11-08 13:14:...|   1|1192|
// |          8|1192|2013-11-11 21:01:...| 112|1276|
// |          8|1276|2013-11-15 16:09:...|1192|1321|
// |          8|1321|2013-11-20 16:42:...|1276|1365|
// |          8|1365|2013-11-23 09:09:...|1321|null|
// |         12|  11|2013-11-05 21:30:...|null|  17|
// |         12|  17|2013-11-05 22:17:...|  11|  18|
// |         12|  18|2013-11-05 22:34:...|  17|  19|
// |         12|  19|2013-11-05 22:38:...|  18|  63|
// |         12|  63|2013-11-06 17:54:...|  19|  65|
// |         12|  65|2013-11-06 18:07:...|  63|  69|
// |         12|  69|2013-11-06 19:41:...|  65|  70|
// |         12|  70|2013-11-06 20:35:...|  69|  89|
// |         12|  89|2013-11-07 19:22:...|  70|  94|
// |         12|  94|2013-11-07 20:42:...|  89| 107|
// |         12| 107|2013-11-08 08:27:...|  94| 122|
// |         12| 122|2013-11-08 20:55:...| 107|1141|
// |         12|1141|2013-11-09 20:50:...| 122|1142|
// +-----------+----+--------------------+----+----+





val countTags = udf((tags: String) => "&lt;".r.findAllMatchIn(tags).length)
val countTags = sqlContext.udf.register("countTags", (tags: String) => "&lt;".r.findAllMatchIn(tags).length)
postsDf.filter('postTypeId === 1).select('tags, countTags('tags) as "tagCnt").show(10, false)
// +-------------------------------------------------------------------+------+
// |tags                                                               |tagCnt|
// +-------------------------------------------------------------------+------+
// |&lt;word-choice&gt;                                                |1     |
// |&lt;english-comparison&gt;&lt;translation&gt;&lt;phrase-request&gt;|3     |
// |&lt;usage&gt;&lt;verbs&gt;                                         |2     |
// |&lt;usage&gt;&lt;tenses&gt;&lt;english-comparison&gt;              |3     |
// |&lt;usage&gt;&lt;punctuation&gt;                                   |2     |
// |&lt;usage&gt;&lt;tenses&gt;                                        |2     |
// |&lt;history&gt;&lt;english-comparison&gt;                          |2     |
// |&lt;idioms&gt;&lt;etymology&gt;                                    |2     |
// |&lt;idioms&gt;&lt;regional&gt;                                     |2     |
// |&lt;grammar&gt;                                                    |1     |
// +-------------------------------------------------------------------+------+

//Section 5.1.4

val cleanPosts = postsDf.na.drop()
cleanPosts.count()
//res0: Long = 222

postsDf.na.fill(Map("viewCount" -> 0))

postsDf.na.replace(Array("id", "acceptedAnswerId"), Map(1177 -> 3000))

//Section 5.1.5

val postsRdd = postsDf.rdd

val postsMapped = postsDf.map(row => Row.fromSeq(
  row.toSeq.updated(3, row.getString(3).replace("&lt;","<").replace("&gt;",">")).
    updated(8, row.getString(8).replace("&lt;","<").replace("&gt;",">"))))
val postsDfNew = sqlContext.createDataFrame(postsMapped, postsDf.schema)

//Section 5.1.6

postsDfNew.groupBy('ownerUserId, 'tags, 'postTypeId).count.orderBy('ownerUserId desc).show(10)
//+-----------+--------------------+----------+-----+
//|ownerUserId|                tags|postTypeId|count|
//+-----------+--------------------+----------+-----+
//|        862|                    |         2|    1|
//|        855|         <resources>|         1|    1|
//|        846|<translation><eng...|         1|    1|
//|        845|<word-meaning><tr...|         1|    1|
//|        842|  <verbs><resources>|         1|    1|
//|        835|    <grammar><verbs>|         1|    1|
//|        833|                    |         2|    1|
//|        833|           <meaning>|         1|    1|
//|        833|<meaning><article...|         1|    1|
//|        814|                    |         2|    1|
//+-----------+--------------------+----------+-----+

postsDfNew.groupBy('ownerUserId).agg(max('lastActivityDate), max('score)).show(10)
postsDfNew.groupBy('ownerUserId).agg(Map("lastActivityDate" -> "max", "score" -> "max")).show(10)
// +-----------+---------------------+----------+
// |ownerUserId|max(lastActivityDate)|max(score)|
// +-----------+---------------------+----------+
// |        431| 2014-02-16 14:16:...|         1|
// |        232| 2014-08-18 20:25:...|         6|
// |        833| 2014-09-03 19:53:...|         4|
// |        633| 2014-05-15 22:22:...|         1|
// |        634| 2014-05-27 09:22:...|         6|
// |        234| 2014-07-12 17:56:...|         5|
// |        235| 2014-08-28 19:30:...|        10|
// |        435| 2014-02-18 13:10:...|        -2|
// |        835| 2014-08-26 15:35:...|         3|
// |         37| 2014-09-13 13:29:...|        23|
// +-----------+---------------------+----------+
postsDfNew.groupBy('ownerUserId).agg(max('lastActivityDate), max('score).gt(5)).show(10)
// +-----------+---------------------+----------------+
// |ownerUserId|max(lastActivityDate)|(max(score) > 5)|
// +-----------+---------------------+----------------+
// |        431| 2014-02-16 14:16:...|           false|
// |        232| 2014-08-18 20:25:...|            true|
// |        833| 2014-09-03 19:53:...|           false|
// |        633| 2014-05-15 22:22:...|           false|
// |        634| 2014-05-27 09:22:...|            true|
// |        234| 2014-07-12 17:56:...|           false|
// |        235| 2014-08-28 19:30:...|            true|
// |        435| 2014-02-18 13:10:...|           false|
// |        835| 2014-08-26 15:35:...|           false|
// |         37| 2014-09-13 13:29:...|            true|
// +-----------+---------------------+----------------+

val smplDf = postsDfNew.where('ownerUserId >= 13 and 'ownerUserId <= 15)
smplDf.groupBy('ownerUserId, 'tags, 'postTypeId).count.show()
// +-----------+----+----------+-----+
// |ownerUserId|tags|postTypeId|count|
// +-----------+----+----------+-----+
// |         15|    |         2|    2|
// |         14|    |         2|    2|
// |         13|    |         2|    1|
// +-----------+----+----------+-----+
smplDf.rollup('ownerUserId, 'tags, 'postTypeId).count.show()
// +-----------+----+----------+-----+
// |ownerUserId|tags|postTypeId|count|
// +-----------+----+----------+-----+
// |         15|    |         2|    2|
// |         13|    |      null|    1|
// |         13|null|      null|    1|
// |         14|    |      null|    2|
// |         13|    |         2|    1|
// |         14|null|      null|    2|
// |         15|    |      null|    2|
// |         14|    |         2|    2|
// |         15|null|      null|    2|
// |       null|null|      null|    5|
// +-----------+----+----------+-----+
smplDf.cube('ownerUserId, 'tags, 'postTypeId).count.show()
// +-----------+----+----------+-----+
// |ownerUserId|tags|postTypeId|count|
// +-----------+----+----------+-----+
// |         15|    |         2|    2|
// |       null|    |         2|    5|
// |         13|    |      null|    1|
// |         15|null|         2|    2|
// |       null|null|         2|    5|
// |         13|null|      null|    1|
// |         14|    |      null|    2|
// |         13|    |         2|    1|
// |         14|null|      null|    2|
// |         15|    |      null|    2|
// |         13|null|         2|    1|
// |       null|    |      null|    5|
// |         14|    |         2|    2|
// |         15|null|      null|    2|
// |       null|null|      null|    5|
// |         14|null|         2|    2|
// +-----------+----+----------+-----+

//Section 5.1.7

val itVotesRaw = sc.textFile("/path/to/italianVotes.csv").map(x => x.split("~"))
val itVotesRows = itVotesRaw.map(row => Row(row(0).toLong, row(1).toLong, row(2).toInt, Timestamp.valueOf(row(3))))
val votesSchema = StructType(Seq(
  StructField("id", LongType, false),
  StructField("postId", LongType, false),
  StructField("voteTypeId", IntegerType, false),
  StructField("creationDate", TimestampType, false))
  )
val votesDf = sqlContext.createDataFrame(itVotesRows, votesSchema)

val postsVotes = postsDf.join(votesDf, postsDf("id") === votesDf("postId"))
val postsVotesOuter = postsDf.join(votesDf, postsDf("id") === votesDf("postId"), "outer")

//Section 5.1.8

sqlContext.sql("SET spark.sql.caseSensitive=true")
sqlContext.setConf("spark.sql.caseSensitive", "true")

//Section 5.2.1

postsDf.registerTempTable("posts_temp")
postsDf.write.saveAsTable("posts")
votesDf.write.saveAsTable("votes")

//Section 5.2.2

val resultDf = sql("select * from posts")

spark-sql> select substring(title, 0, 70) from posts3 where postTypeId = 1 order by creationDate desc limit 3;
$ spark-sql -e "select substring(title, 0, 70) from posts where postTypeId = 1 order by creationDate desc limit 3"

//Section 5.3.1

postsDf.write.format("json").saveAsTable("postsjson")

sql("select * from postsjson")

val props = new java.util.Properties()
props.setProperty("user", "user")
props.setProperty("password", "password")
postsDf.write.jdbc("jdbc:postgresql://postgresrv/mydb", "posts", props)

//Section 5.3.2

val postsDf = sqlContext.read.table("posts")
val postsDf = sqlContext.table("posts")

val result = sqlContext.read.jdbc("jdbc:postgresql://postgresrv/mydb", "posts", Array("viewCount > 3"), props)

sql("CREATE TEMPORARY TABLE postsjdbc "+
  "USING org.apache.spark.sql.jdbc "+
  "OPTIONS ("+
    "url 'jdbc:postgresql://postgresrv/mydb',"+
    "dbtable 'posts',"+
    "user 'user',"+
    "password 'password')")

sql("CREATE TEMPORARY TABLE postsParquet "+
  "USING org.apache.spark.sql.parquet "+
  "OPTIONS (path '/path/to/parquet_file')")
val resParq = sql("select * from postsParquet")

//Section 5.4

val postsFiltered = postsDf.filter('postTypeId === 1).withColumn("ratio", 'viewCount / 'score).where('ratio < 35)
// postsFiltered.explain(true)
// == Parsed Logical Plan ==
// 'Filter ('ratio < 35)
//  Project [commentCount#0,lastActivityDate#1,ownerUserId#2L,body#3,score#4,creationDate#5,viewCount#6,title#7,tags#8,answerCount#9,acceptedAnswerId#10L,postTypeId#11L,id#12L,(cast(viewCount#6 as double) / cast(score#4 as double)) AS ratio#21]
//   Filter (postTypeId#11L = cast(1 as bigint))
//    Project [commentCount#0,lastActivityDate#1,ownerUserId#2L,body#3,score#4,creationDate#5,viewCount#6,title#7,tags#8,answerCount#9,acceptedAnswerId#10L,postTypeId#11L,id#12L]
//     Subquery posts
//      Relation[commentCount#0,lastActivityDate#1,ownerUserId#2L,body#3,score#4,creationDate#5,viewCount#6,title#7,tags#8,answerCount#9,acceptedAnswerId#10L,postTypeId#11L,id#12L] ParquetRelation[/path/to/posts/posts]

// == Analyzed Logical Plan ==
// commentCount: int, lastActivityDate: timestamp, ownerUserId: bigint, body: string, score: int, creationDate: timestamp, viewCount: int, title: string, tags: string, answerCount: int, acceptedAnswerId: bigint, postTypeId: bigint, id: bigint, ratio: double
// Filter (ratio#21 < cast(35 as double))
//  Project [commentCount#0,lastActivityDate#1,ownerUserId#2L,body#3,score#4,creationDate#5,viewCount#6,title#7,tags#8,answerCount#9,acceptedAnswerId#10L,postTypeId#11L,id#12L,(cast(viewCount#6 as double) / cast(score#4 as double)) AS ratio#21]
//   Filter (postTypeId#11L = cast(1 as bigint))
//    Project [commentCount#0,lastActivityDate#1,ownerUserId#2L,body#3,score#4,creationDate#5,viewCount#6,title#7,tags#8,answerCount#9,acceptedAnswerId#10L,postTypeId#11L,id#12L]
//     Subquery posts
//      Relation[commentCount#0,lastActivityDate#1,ownerUserId#2L,body#3,score#4,creationDate#5,viewCount#6,title#7,tags#8,answerCount#9,acceptedAnswerId#10L,postTypeId#11L,id#12L] ParquetRelation[/path/to/posts/posts]

// == Optimized Logical Plan ==
// Project [commentCount#0,lastActivityDate#1,ownerUserId#2L,body#3,score#4,creationDate#5,viewCount#6,title#7,tags#8,answerCount#9,acceptedAnswerId#10L,postTypeId#11L,id#12L,(cast(viewCount#6 as double) / cast(score#4 as double)) AS ratio#21]
//  Filter ((postTypeId#11L = 1) && ((cast(viewCount#6 as double) / cast(score#4 as double)) < 35.0))
//   Relation[commentCount#0,lastActivityDate#1,ownerUserId#2L,body#3,score#4,creationDate#5,viewCount#6,title#7,tags#8,answerCount#9,acceptedAnswerId#10L,postTypeId#11L,id#12L] ParquetRelation[/path/to/posts/posts]

// == Physical Plan ==
// Project [commentCount#0,lastActivityDate#1,ownerUserId#2L,body#3,score#4,creationDate#5,viewCount#6,title#7,tags#8,answerCount#9,acceptedAnswerId#10L,postTypeId#11L,id#12L,(cast(viewCount#6 as double) / cast(score#4 as double)) AS ratio#21]
//  Filter ((postTypeId#11L = 1) && ((cast(viewCount#6 as double) / cast(score#4 as double)) < 35.0))
//   Scan ParquetRelation[/path/to/posts][ownerUserId#2L,tags#8,body#3,id#12L,score#4,postTypeId#11L,lastActivityDate#1,creationDate#5,answerCount#9,title#7,viewCount#6,acceptedAnswerId#10L,commentCount#0]

// Code Generation: true


