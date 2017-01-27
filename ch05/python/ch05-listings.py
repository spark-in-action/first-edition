
from __future__ import print_function

# Section 5.1.1

itPostsRows = sc.textFile("first-edition/ch05/italianPosts.csv")
itPostsSplit = itPostsRows.map(lambda x: x.split("~"))

itPostsRDD = itPostsSplit.map(lambda x: (x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8],x[9],x[10],x[11],x[12]))
itPostsDFrame = itPostsRDD.toDF()
itPostsDFrame.show(10)
# +---+--------------------+---+--------------------+---+--------------------+----+--------------------+--------------------+----+----+---+----+
# | _1|                  _2| _3|                  _4| _5|                  _6|  _7|                  _8|                  _9| _10| _11|_12| _13|
# +---+--------------------+---+--------------------+---+--------------------+----+--------------------+--------------------+----+----+---+----+
# |  4|2013-11-11 18:21:...| 17|&lt;p&gt;The infi...| 23|2013-11-10 19:37:...|null|                    |                    |null|null|  2|1165|
# |  5|2013-11-10 20:31:...| 12|&lt;p&gt;Come cre...|  1|2013-11-10 19:44:...|  61|Cosa sapreste dir...| &lt;word-choice&gt;|   1|null|  1|1166|
# |  2|2013-11-10 20:31:...| 17|&lt;p&gt;Il verbo...|  5|2013-11-10 19:58:...|null|                    |                    |null|null|  2|1167|
# |  1|2014-07-25 13:15:...|154|&lt;p&gt;As part ...| 11|2013-11-10 22:03:...| 187|Ironic constructi...|&lt;english-compa...|   4|1170|  1|1168|
# |  0|2013-11-10 22:15:...| 70|&lt;p&gt;&lt;em&g...|  3|2013-11-10 22:15:...|null|                    |                    |null|null|  2|1169|
# |  2|2013-11-10 22:17:...| 17|&lt;p&gt;There's ...|  8|2013-11-10 22:17:...|null|                    |                    |null|null|  2|1170|
# |  1|2013-11-11 09:51:...| 63|&lt;p&gt;As other...|  3|2013-11-11 09:51:...|null|                    |                    |null|null|  2|1171|
# |  1|2013-11-12 23:57:...| 63|&lt;p&gt;The expr...|  1|2013-11-11 10:09:...|null|                    |                    |null|null|  2|1172|
# |  9|2014-01-05 11:13:...| 63|&lt;p&gt;When I w...|  5|2013-11-11 10:28:...| 122|Is &quot;scancell...|&lt;usage&gt;&lt;...|   3|1181|  1|1173|
# |  0|2013-11-11 10:58:...| 18|&lt;p&gt;Wow, wha...|  5|2013-11-11 10:58:...|null|                    |                    |null|null|  2|1174|
# +---+--------------------+---+--------------------+---+--------------------+----+--------------------+--------------------+----+----+---+----+
# only showing top 10 rows

itPostsDF = itPostsRDD.toDF(["commentCount", "lastActivityDate", "ownerUserId", "body", "score", "creationDate", "viewCount", "title", "tags", "answerCount", "acceptedAnswerId", "postTypeId", "id"])

itPostsDF.printSchema()
# root
#  |-- commentCount: string (nullable = true)
#  |-- lastActivityDate: string (nullable = true)
#  |-- ownerUserId: string (nullable = true)
#  |-- body: string (nullable = true)
#  |-- score: string (nullable = true)
#  |-- creationDate: string (nullable = true)
#  |-- viewCount: string (nullable = true)
#  |-- title: string (nullable = true)
#  |-- tags: string (nullable = true)
#  |-- answerCount: string (nullable = true)
#  |-- acceptedAnswerId: string (nullable = true)
#  |-- postTypeId: string (nullable = true)
#  |-- id: string (nullable = true)


from pyspark.sql import Row
from datetime import datetime
def toIntSafe(inval):
  try:
    return int(inval)
  except ValueError:
    return None

def toTimeSafe(inval):
  try:
    return datetime.strptime(inval, "%Y-%m-%d %H:%M:%S.%f")
  except ValueError:
    return None

def toLongSafe(inval):
  try:
    return long(inval)
  except ValueError:
    return None

def stringToPost(row):
  r = row.encode('utf8').split("~")
  return Row(
    toIntSafe(r[0]),
    toTimeSafe(r[1]),
    toIntSafe(r[2]),
    r[3],
    toIntSafe(r[4]),
    toTimeSafe(r[5]),
    toIntSafe(r[6]),
    toIntSafe(r[7]),
    r[8],
    toIntSafe(r[9]),
    toLongSafe(r[10]),
    toLongSafe(r[11]),
    long(r[12]))

from pyspark.sql.types import *
postSchema = StructType([
  StructField("commentCount", IntegerType(), True),
  StructField("lastActivityDate", TimestampType(), True),
  StructField("ownerUserId", LongType(), True),
  StructField("body", StringType(), True),
  StructField("score", IntegerType(), True),
  StructField("creationDate", TimestampType(), True),
  StructField("viewCount", IntegerType(), True),
  StructField("title", StringType(), True),
  StructField("tags", StringType(), True),
  StructField("answerCount", IntegerType(), True),
  StructField("acceptedAnswerId", LongType(), True),
  StructField("postTypeId", LongType(), True),
  StructField("id", LongType(), False)
  ])

rowRDD = itPostsRows.map(lambda x: stringToPost(x))
itPostsDFStruct = sqlContext.createDataFrame(rowRDD, postSchema)
itPostsDFStruct.printSchema()
itPostsDFStruct.columns
itPostsDFStruct.dtypes

#Section 5.1.2

postsDf = itPostsDFStruct
postsIdBody = postsDf.select("id", "body")

postsIdBody = postsDf.select(postsDf["id"], postsDf["body"])

postIds = postsIdBody.drop("body")

from pyspark.sql.functions import *
postsIdBody.filter(instr(postsIdBody["body"], "Italiano") > 0).count()

noAnswer = postsDf.filter((postsDf["postTypeId"] == 1) & isnull(postsDf["acceptedAnswerId"]))

firstTenQs = postsDf.filter(postsDf["postTypeId"] == 1).limit(10)
firstTenQsRn = firstTenQs.withColumnRenamed("ownerUserId", "owner")

postsDf.filter(postsDf.postTypeId == 1).withColumn("ratio", postsDf.viewCount / postsDf.score).where("ratio < 35").show()
#+------------+--------------------+-----------+--------------------+-----+--------------------+---------+-----+--------------------+-----------+----------------+----------+----+-------------------+
#|commentCount|    lastActivityDate|ownerUserId|                body|score|        creationDate|viewCount|title|                tags|answerCount|acceptedAnswerId|postTypeId|  id|              ratio|
#+------------+--------------------+-----------+--------------------+-----+--------------------+---------+-----+--------------------+-----------+----------------+----------+----+-------------------+
#|           5|2013-11-21 14:04:...|          8|&lt;p&gt;The use ...|   13|2013-11-11 21:01:...|      142| null|&lt;prepositions&...|          2|            1212|         1|1192| 10.923076923076923|
#|           0|2013-11-12 09:26:...|         17|&lt;p&gt;When wri...|    5|2013-11-11 21:01:...|       70| null|&lt;punctuation&g...|          4|            1195|         1|1193|               14.0|
#|           1|2013-11-12 12:53:...|         99|&lt;p&gt;I can't ...|   -3|2013-11-12 10:57:...|       68| null|&lt;grammar&gt;&l...|          3|            1216|         1|1203|-22.666666666666668|
#|           3|2014-09-11 14:37:...|         63|&lt;p&gt;The plur...|    5|2013-11-12 13:34:...|       59| null|&lt;plural&gt;&lt...|          1|            1227|         1|1221|               11.8|
#|           1|2013-11-12 13:49:...|         63|&lt;p&gt;I rememb...|    6|2013-11-12 13:38:...|       53| null|&lt;usage&gt;&lt;...|          1|            1223|         1|1222|  8.833333333333334|
#|           5|2013-11-13 00:32:...|        159|&lt;p&gt;Girando ...|    6|2013-11-12 23:50:...|       88| null|&lt;grammar&gt;&l...|          1|            1247|         1|1246| 14.666666666666666|
#|           0|2013-11-14 00:54:...|        159|&lt;p&gt;Mi A¨ ca...|    7|2013-11-14 00:19:...|       70| null|       &lt;verbs&gt;|          1|            null|         1|1258|               10.0|
#|           1|2013-11-15 12:17:...|         18|&lt;p&gt;Clearly ...|    7|2013-11-14 01:21:...|       68| null|&lt;grammar&gt;&l...|          2|            null|         1|1262|  9.714285714285714|
#|           0|2013-11-14 21:14:...|         79|&lt;p&gt;Alle ele...|    8|2013-11-14 20:16:...|       96| null|&lt;grammar&gt;&l...|          1|            1271|         1|1270|               12.0|
#|           0|2013-11-15 17:12:...|         63|&lt;p&gt;In Itali...|    8|2013-11-15 14:54:...|       68| null|&lt;usage&gt;&lt;...|          1|            1277|         1|1275|                8.5|
#|           3|2013-11-19 18:08:...|          8|&lt;p&gt;The Ital...|    6|2013-11-15 16:09:...|       87| null|&lt;grammar&gt;&l...|          1|            null|         1|1276|               14.5|
#|           1|2014-08-14 13:13:...|         12|&lt;p&gt;When I s...|    5|2013-11-16 09:36:...|       74| null|&lt;regional&gt;&...|          3|            null|         1|1279|               14.8|
#|          10|2014-03-15 08:25:...|        176|&lt;p&gt;In Engli...|   12|2013-11-16 11:13:...|      148| null|&lt;punctuation&g...|          2|            1286|         1|1285| 12.333333333333334|
#|           2|2013-11-17 15:54:...|         79|&lt;p&gt;Al di fu...|    7|2013-11-16 13:16:...|       70| null|     &lt;accents&gt;|          2|            null|         1|1287|               10.0|
#|           1|2013-11-16 19:05:...|        176|&lt;p&gt;Often ti...|   12|2013-11-16 14:16:...|      106| null|&lt;verbs&gt;&lt;...|          1|            null|         1|1290|  8.833333333333334|
#|           4|2013-11-17 15:50:...|         22|&lt;p&gt;The verb...|    6|2013-11-17 14:30:...|       66| null|&lt;verbs&gt;&lt;...|          1|            null|         1|1298|               11.0|
#|           0|2014-09-12 10:55:...|          8|&lt;p&gt;Wikipedi...|   10|2013-11-20 16:42:...|      145| null|&lt;orthography&g...|          5|            1336|         1|1321|               14.5|
#|           2|2013-11-21 12:09:...|         22|&lt;p&gt;La parol...|    5|2013-11-20 20:48:...|       49| null|&lt;usage&gt;&lt;...|          1|            1338|         1|1324|                9.8|
#|           0|2013-11-22 13:34:...|        114|&lt;p&gt;There ar...|    7|2013-11-20 20:53:...|       69| null|   &lt;homograph&gt;|          2|            1330|         1|1325|  9.857142857142858|
#|           6|2013-11-26 19:12:...|         12|&lt;p&gt;Sento ch...|   -3|2013-11-21 21:12:...|       79| null|  &lt;word-usage&gt;|          2|            null|         1|1347|-26.333333333333332|
#+------------+--------------------+-----------+--------------------+-----+--------------------+---------+-----+--------------------+-----------+----------------+----------+----+-------------------+
# only showing top 20 rows

#The 10 most recently modified questions:
postsDf.filter(postsDf.postTypeId == 1).orderBy(postsDf.lastActivityDate.desc()).limit(10).show()

#Section 5.1.3

from pyspark.sql.functions import *
postsDf.filter(postsDf.postTypeId == 1).withColumn("activePeriod", datediff(postsDf.lastActivityDate, postsDf.creationDate)).orderBy(desc("activePeriod")).head().body.replace("&lt;","<").replace("&gt;",">")
#<p>The plural of <em>braccio</em> is <em>braccia</em>, and the plural of <em>avambraccio</em> is <em>avambracci</em>.</p><p>Why are the plural of those words so different, if they both are referring to parts of the human body, and <em>avambraccio</em> derives from <em>braccio</em>?</p>

postsDf.select(avg(postsDf.score), max(postsDf.score), count(postsDf.score)).show()


from pyspark.sql.window import Window
winDf = postsDf.filter(postsDf.postTypeId == 1).select(postsDf.ownerUserId, postsDf.acceptedAnswerId, postsDf.score, max(postsDf.score).over(Window.partitionBy(postsDf.ownerUserId)).alias("maxPerUser"))
winDf.withColumn("toMax", winDf.maxPerUser - winDf.score).show(10)
# +-----------+----------------+-----+----------+-----+
# |ownerUserId|acceptedAnswerId|score|maxPerUser|toMax|
# +-----------+----------------+-----+----------+-----+
# |        232|            2185|    6|         6|    0|
# |        833|            2277|    4|         4|    0|
# |        833|            null|    1|         4|    3|
# |        235|            2004|   10|        10|    0|
# |        835|            2280|    3|         3|    0|
# |         37|            null|    4|        13|    9|
# |         37|            null|   13|        13|    0|
# |         37|            2313|    8|        13|    5|
# |         37|              20|   13|        13|    0|
# |         37|            null|    4|        13|    9|
# +-----------+----------------+-----+----------+-----+

postsDf.filter(postsDf.postTypeId == 1).select(postsDf.ownerUserId, postsDf.id, postsDf.creationDate, lag(postsDf.id, 1).over(Window.partitionBy(postsDf.ownerUserId).orderBy(postsDf.creationDate)).alias("prev"), lead(postsDf.id, 1).over(Window.partitionBy(postsDf.ownerUserId).orderBy(postsDf.creationDate)).alias("next")).orderBy(postsDf.ownerUserId, postsDf.id).show()
# +-----------+----+--------------------+----+----+
# |ownerUserId|  id|        creationDate|prev|next|
# +-----------+----+--------------------+----+----+
# |          4|1637|2014-01-24 06:51:...|null|null|
# |          8|   1|2013-11-05 20:22:...|null| 112|
# |          8| 112|2013-11-08 13:14:...|   1|1192|
# |          8|1192|2013-11-11 21:01:...| 112|1276|
# |          8|1276|2013-11-15 16:09:...|1192|1321|
# |          8|1321|2013-11-20 16:42:...|1276|1365|
# |          8|1365|2013-11-23 09:09:...|1321|null|
# |         12|  11|2013-11-05 21:30:...|null|  17|
# |         12|  17|2013-11-05 22:17:...|  11|  18|
# |         12|  18|2013-11-05 22:34:...|  17|  19|
# |         12|  19|2013-11-05 22:38:...|  18|  63|
# |         12|  63|2013-11-06 17:54:...|  19|  65|
# |         12|  65|2013-11-06 18:07:...|  63|  69|
# |         12|  69|2013-11-06 19:41:...|  65|  70|
# |         12|  70|2013-11-06 20:35:...|  69|  89|
# |         12|  89|2013-11-07 19:22:...|  70|  94|
# |         12|  94|2013-11-07 20:42:...|  89| 107|
# |         12| 107|2013-11-08 08:27:...|  94| 122|
# |         12| 122|2013-11-08 20:55:...| 107|1141|
# |         12|1141|2013-11-09 20:50:...| 122|1142|
# +-----------+----+--------------------+----+----+


countTags = udf(lambda (tags): tags.count("&lt;"), IntegerType())
postsDf.filter(postsDf.postTypeId == 1).select("tags", countTags(postsDf.tags).alias("tagCnt")).show(10, False)
# +-------------------------------------------------------------------+------+
# |tags                                                               |tagCnt|
# +-------------------------------------------------------------------+------+
# |&lt;word-choice&gt;                                                |1     |
# |&lt;english-comparison&gt;&lt;translation&gt;&lt;phrase-request&gt;|3     |
# |&lt;usage&gt;&lt;verbs&gt;                                         |2     |
# |&lt;usage&gt;&lt;tenses&gt;&lt;english-comparison&gt;              |3     |
# |&lt;usage&gt;&lt;punctuation&gt;                                   |2     |
# |&lt;usage&gt;&lt;tenses&gt;                                        |2     |
# |&lt;history&gt;&lt;english-comparison&gt;                          |2     |
# |&lt;idioms&gt;&lt;etymology&gt;                                    |2     |
# |&lt;idioms&gt;&lt;regional&gt;                                     |2     |
# |&lt;grammar&gt;                                                    |1     |
# +-------------------------------------------------------------------+------+

#Section 5.1.4

cleanPosts = postsDf.na.drop()
cleanPosts.count()

postsDf.na.fill({"viewCount": 0}).show()

postsDf.na.replace(1177, 3000, ["id", "acceptedAnswerId"]).show()

#Section 5.1.5

postsRdd = postsDf.rdd

def replaceLtGt(row):
	return Row(
	  commentCount = row.commentCount,
    lastActivityDate = row.lastActivityDate,
    ownerUserId = row.ownerUserId,
    body = row.body.replace("&lt;","<").replace("&gt;",">"),
    score = row.score,
    creationDate = row.creationDate,
    viewCount = row.viewCount,
    title = row.title,
    tags = row.tags.replace("&lt;","<").replace("&gt;",">"),
    answerCount = row.answerCount,
    acceptedAnswerId = row.acceptedAnswerId,
    postTypeId = row.postTypeId,
    id = row.id)

postsMapped = postsRdd.map(replaceLtGt)

def sortSchema(schema):
	fields = {f.name: f for f in schema.fields}
	names = sorted(fields.keys())
	return StructType([fields[f] for f in names])

postsDfNew = sqlContext.createDataFrame(postsMapped, sortSchema(postsDf.schema))

#Section 5.1.6

postsDfNew.groupBy(postsDfNew.ownerUserId, postsDfNew.tags, postsDfNew.postTypeId).count().orderBy(postsDfNew.ownerUserId.desc()).show(10)
#+-----------+--------------------+----------+-----+
#|ownerUserId|                tags|postTypeId|count|
#+-----------+--------------------+----------+-----+
#|        862|                    |         2|    1|
#|        855|         <resources>|         1|    1|
#|        846|<translation><eng...|         1|    1|
#|        845|<word-meaning><tr...|         1|    1|
#|        842|  <verbs><resources>|         1|    1|
#|        835|    <grammar><verbs>|         1|    1|
#|        833|                    |         2|    1|
#|        833|           <meaning>|         1|    1|
#|        833|<meaning><article...|         1|    1|
#|        814|                    |         2|    1|
#+-----------+--------------------+----------+-----+

postsDfNew.groupBy(postsDfNew.ownerUserId).agg(max(postsDfNew.lastActivityDate), max(postsDfNew.score)).show(10)
postsDfNew.groupBy(postsDfNew.ownerUserId).agg({"lastActivityDate": "max", "score": "max"}).show(10)
# +-----------+---------------------+----------+
# |ownerUserId|max(lastActivityDate)|max(score)|
# +-----------+---------------------+----------+
# |        431| 2014-02-16 14:16:...|         1|
# |        232| 2014-08-18 20:25:...|         6|
# |        833| 2014-09-03 19:53:...|         4|
# |        633| 2014-05-15 22:22:...|         1|
# |        634| 2014-05-27 09:22:...|         6|
# |        234| 2014-07-12 17:56:...|         5|
# |        235| 2014-08-28 19:30:...|        10|
# |        435| 2014-02-18 13:10:...|        -2|
# |        835| 2014-08-26 15:35:...|         3|
# |         37| 2014-09-13 13:29:...|        23|
# +-----------+---------------------+----------+
postsDfNew.groupBy(postsDfNew.ownerUserId).agg(max(postsDfNew.lastActivityDate), max(postsDfNew.score) > 5).show(10)
# +-----------+---------------------+----------------+
# |ownerUserId|max(lastActivityDate)|(max(score) > 5)|
# +-----------+---------------------+----------------+
# |        431| 2014-02-16 14:16:...|           false|
# |        232| 2014-08-18 20:25:...|            true|
# |        833| 2014-09-03 19:53:...|           false|
# |        633| 2014-05-15 22:22:...|           false|
# |        634| 2014-05-27 09:22:...|            true|
# |        234| 2014-07-12 17:56:...|           false|
# |        235| 2014-08-28 19:30:...|            true|
# |        435| 2014-02-18 13:10:...|           false|
# |        835| 2014-08-26 15:35:...|           false|
# |         37| 2014-09-13 13:29:...|            true|
# +-----------+---------------------+----------------+

smplDf = postsDfNew.where((postsDfNew.ownerUserId >= 13) & (postsDfNew.ownerUserId <= 15))
smplDf.groupBy(smplDf.ownerUserId, smplDf.tags, smplDf.postTypeId).count().show()
# +-----------+----+----------+-----+
# |ownerUserId|tags|postTypeId|count|
# +-----------+----+----------+-----+
# |         15|    |         2|    2|
# |         14|    |         2|    2|
# |         13|    |         2|    1|
# +-----------+----+----------+-----+
smplDf.rollup(smplDf.ownerUserId, smplDf.tags, smplDf.postTypeId).count().show()
# +-----------+----+----------+-----+
# |ownerUserId|tags|postTypeId|count|
# +-----------+----+----------+-----+
# |         15|    |         2|    2|
# |         13|    |      null|    1|
# |         13|null|      null|    1|
# |         14|    |      null|    2|
# |         13|    |         2|    1|
# |         14|null|      null|    2|
# |         15|    |      null|    2|
# |         14|    |         2|    2|
# |         15|null|      null|    2|
# |       null|null|      null|    5|
# +-----------+----+----------+-----+
smplDf.cube(smplDf.ownerUserId, smplDf.tags, smplDf.postTypeId).count().show()
# +-----------+----+----------+-----+
# |ownerUserId|tags|postTypeId|count|
# +-----------+----+----------+-----+
# |         15|    |         2|    2|
# |       null|    |         2|    5|
# |         13|    |      null|    1|
# |         15|null|         2|    2|
# |       null|null|         2|    5|
# |         13|null|      null|    1|
# |         14|    |      null|    2|
# |         13|    |         2|    1|
# |         14|null|      null|    2|
# |         15|    |      null|    2|
# |         13|null|         2|    1|
# |       null|    |      null|    5|
# |         14|    |         2|    2|
# |         15|null|      null|    2|
# |       null|null|      null|    5|
# |         14|null|         2|    2|
# +-----------+----+----------+-----+

#Section 5.1.7

itVotesRaw = sc.textFile("first-edition/ch05/italianVotes.csv").map(lambda x: x.split("~"))
itVotesRows = itVotesRaw.map(lambda row: Row(id=long(row[0]), postId=long(row[1]), voteTypeId=int(row[2]), creationDate=datetime.strptime(row[3], "%Y-%m-%d %H:%M:%S.%f")))
votesSchema = StructType([
  StructField("creationDate", TimestampType(), False),
  StructField("id", LongType(), False),
  StructField("postId", LongType(), False),
  StructField("voteTypeId", IntegerType(), False)
  ])  

votesDf = sqlContext.createDataFrame(itVotesRows, votesSchema)

postsVotes = postsDf.join(votesDf, postsDf.id == votesDf.postId)
postsVotesOuter = postsDf.join(votesDf, postsDf.id == votesDf.postId, "outer")

#Section 5.1.8

sqlContext.sql("SET spark.sql.caseSensitive=true")
sqlContext.setConf("spark.sql.caseSensitive", "true")

#Section 5.2.1

postsDf.registerTempTable("posts_temp")
postsDf.write.saveAsTable("posts")
votesDf.write.saveAsTable("votes")

#Section 5.2.2

resultDf = sqlContext.sql("select * from posts")

spark-sql> select substring(title, 0, 70) from posts where postTypeId = 1 order by creationDate desc limit 3;
$ spark-sql -e "select substring(title, 0, 70) from posts where postTypeId = 1 order by creationDate desc limit 3"

#Section 5.3.1

postsDf.write.format("json").saveAsTable("postsjson")

sqlContext.sql("select * from postsjson")

props = {"user": "user", "password": "password"}
postsDf.write.jdbc("jdbc:postgresql:#postgresrv/mydb", "posts", properties=props)

#Section 5.3.2

postsDf = sqlContext.read.table("posts")
postsDf = sqlContext.table("posts")

result = sqlContext.read.jdbc("jdbc:postgresql:#postgresrv/mydb", "posts", predicates=["viewCount > 3"], properties=props)

sqlContext.sql("CREATE TEMPORARY TABLE postsjdbc "+
  "USING org.apache.spark.sql.jdbc "+
  "OPTIONS ("+
    "url 'jdbc:postgresql:#postgresrv/mydb',"+
    "dbtable 'posts',"+
    "user 'user',"+
    "password 'password')")

sqlContext.sql("CREATE TEMPORARY TABLE postsParquet "+
  "USING org.apache.spark.sql.parquet "+
  "OPTIONS (path '/path/to/parquet_file')")
resParq = sql("select * from postsParquet")

#Section 5.4

postsRatio = postsDf.filter(postsDf.postTypeId == 1).withColumn("ratio", postsDf.viewCount / postsDf.score)
postsFiltered = postsRatio.where(postsRatio.ratio < 35)
postsFiltered.explain(True)
# == Parsed Logical Plan ==
# 'Filter (ratio#314 < 35)
# +- Project [commentCount#285,lastActivityDate#286,ownerUserId#287L,body#288,score#289,creationDate#290,viewCount#291,title#292,tags#293,answerCount#294,acceptedAnswerId#295L,postTypeId#296L,id#297L,(cast(viewCount#291 as double) / cast(score#289 as double)) AS ratio#314]
#    +- Filter (postTypeId#296L = cast(1 as bigint))
#       +- Subquery posts
#          +- Relation[commentCount#285,lastActivityDate#286,ownerUserId#287L,body#288,score#289,creationDate#290,viewCount#291,title#292,tags#293,answerCount#294,acceptedAnswerId#295L,postTypeId#296L,id#297L] ParquetRelation
# 
# == Analyzed Logical Plan ==
# commentCount: int, lastActivityDate: timestamp, ownerUserId: bigint, body: string, score: int, creationDate: timestamp, viewCount: int, title: string, tags: string, answerCount: int, acceptedAnswerId: bigint, postTypeId: bigint, id: bigint, ratio: double
# Filter (ratio#314 < cast(35 as double))
# +- Project [commentCount#285,lastActivityDate#286,ownerUserId#287L,body#288,score#289,creationDate#290,viewCount#291,title#292,tags#293,answerCount#294,acceptedAnswerId#295L,postTypeId#296L,id#297L,(cast(viewCount#291 as double) / cast(score#289 as double)) AS ratio#314]
#    +- Filter (postTypeId#296L = cast(1 as bigint))
#       +- Subquery posts
#          +- Relation[commentCount#285,lastActivityDate#286,ownerUserId#287L,body#288,score#289,creationDate#290,viewCount#291,title#292,tags#293,answerCount#294,acceptedAnswerId#295L,postTypeId#296L,id#297L] ParquetRelation
# 
# == Optimized Logical Plan ==
# Project [commentCount#285,lastActivityDate#286,ownerUserId#287L,body#288,score#289,creationDate#290,viewCount#291,title#292,tags#293,answerCount#294,acceptedAnswerId#295L,postTypeId#296L,id#297L,(cast(viewCount#291 as double) / cast(score#289 as double)) AS ratio#314]
# +- Filter ((postTypeId#296L = 1) && ((cast(viewCount#291 as double) / cast(score#289 as double)) < 35.0))
#    +- Relation[commentCount#285,lastActivityDate#286,ownerUserId#287L,body#288,score#289,creationDate#290,viewCount#291,title#292,tags#293,answerCount#294,acceptedAnswerId#295L,postTypeId#296L,id#297L] ParquetRelation
# 
# == Physical Plan ==
# Project [commentCount#285,lastActivityDate#286,ownerUserId#287L,body#288,score#289,creationDate#290,viewCount#291,title#292,tags#293,answerCount#294,acceptedAnswerId#295L,postTypeId#296L,id#297L,(cast(viewCount#291 as double) / cast(score#289 as double)) AS ratio#314]
# +- Filter (((cast(viewCount#291 as double) / cast(score#289 as double)) < 35.0) && (postTypeId#296L = 1))
#    +- Scan ParquetRelation[lastActivityDate#286,commentCount#285,acceptedAnswerId#295L,title#292,id#297L,postTypeId#296L,ownerUserId#287L,score#289,tags#293,viewCount#291,body#288,creationDate#290,answerCount#294] InputPaths: file:/user/hive/warehouse/posts, PushedFilters: [EqualTo(postTypeId,1)]
