package org.sia;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;

import com.google.common.collect.ImmutableMap;

import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.hive.HiveContext;

import scala.Tuple2;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.bitbucket.dollar.Dollar.*;

public class Chapter5App {
  public static void main(String[] args) throws IOException {
      JavaSparkContext sc = new JavaSparkContext(new SparkConf());
      HiveContext sqlContext = new HiveContext(sc);

      System.out.println("\n\n---- Section 5.1.1 ----");

      JavaRDD<String> itPostsRows = sc.textFile("first-edition/ch05/italianPosts.csv");
      JavaRDD<String[]> itPostsSplit = itPostsRows.map((String x) -> x.split("~"));
      JavaRDD<Post> itPostsRDD = itPostsSplit.flatMap((String[] x) -> {
        List<Post> l = new ArrayList<Post>();
        try {
          Post p = new Post(x);
          l.add(p);
        }
        catch(Exception e) {
          System.out.println("Wrong line format: "+x);
        }
        return l;
      });

      DataFrame itPostsDFCase = sqlContext.createDataFrame(itPostsRDD, Post.class);
      itPostsDFCase.show(10);

      itPostsDFCase.printSchema();

      StructField[] schemaFields = {
        DataTypes.createStructField("commentCount", DataTypes.IntegerType, true),
          DataTypes.createStructField("lastActivityDate", DataTypes.TimestampType, true),
          DataTypes.createStructField("ownerUserId", DataTypes.LongType, true),
          DataTypes.createStructField("body", DataTypes.StringType, true),
          DataTypes.createStructField("score", DataTypes.IntegerType, true),
          DataTypes.createStructField("creationDate", DataTypes.TimestampType, true),
          DataTypes.createStructField("viewCount", DataTypes.IntegerType, true),
          DataTypes.createStructField("title", DataTypes.StringType, true),
          DataTypes.createStructField("tags", DataTypes.StringType, true),
          DataTypes.createStructField("answerCount", DataTypes.IntegerType, true),
          DataTypes.createStructField("acceptedAnswerId", DataTypes.LongType, true),
          DataTypes.createStructField("postTypeId", DataTypes.LongType, true),
          DataTypes.createStructField("id", DataTypes.LongType, false)};

      StructType postSchema = DataTypes.createStructType(schemaFields);

      JavaRDD<Row> rowRDD = itPostsSplit.map((String[] split) -> RowFactory.create(parseRow(split)));
      DataFrame itPostsDFStruct = sqlContext.createDataFrame(rowRDD, postSchema);
      System.out.println();
      System.out.println("itPostsDFStruct columns: "+String.join(", ", itPostsDFStruct.columns()));
      System.out.println("itPostsDFStruct dtypes: ");
      for(Tuple2<String, String> t : itPostsDFStruct.dtypes())
        System.out.println(t._1+", "+t._2);

      System.out.println("\n\n---- Section 5.1.2 ----");

      DataFrame postsDf = itPostsDFStruct;
      DataFrame postsIdBody = postsDf.select("id", "body");
//      DataFrame postsIdBody = postsDf.select(postsDf.col("id"), postsDf.col("body"));

      DataFrame postIds = postsIdBody.drop("body");

      System.out.println("\nPosts with 'Italiano': "+postsIdBody.filter(postsIdBody.col("body").contains("Italiano")).count());

    DataFrame noAnswer = postsDf.filter(postsDf.col("postTypeId").equalTo(1).and(col("acceptedAnswerId").isNull()));
    noAnswer.show();

    DataFrame firstTenQs = postsDf.filter(postsDf.col("postTypeId").equalTo(1)).limit(10);
    DataFrame firstTenQsRn = firstTenQs.withColumnRenamed("ownerUserId", "owner");

    System.out.println("\nWith ratio column: ");
    postsDf.filter("postTypeId = 1").withColumn("ratio", postsDf.col("viewCount").divide(postsDf.col("score"))).where("ratio < 35").show();
    System.out.println("\nThe 10 most recently modified questions: ");
    postsDf.filter("postTypeId = 1").orderBy(postsDf.col("lastActivityDate").desc()).limit(10).show();

      System.out.println("\n\n---- Section 5.1.3 ----");
      System.out.println("\nThe longest active post:");
      postsDf.filter(postsDf.col("postTypeId").equalTo(1)).withColumn("activePeriod", datediff(col("lastActivityDate"), col("creationDate"))).
        orderBy(col("activePeriod").desc()).head().getString(3).replace("&lt;","<").replace("&gt;",">");

      System.out.println("\nAverage and maximum score of all questions, total number of questions:");
    postsDf.select(avg(postsDf.col("score")), max(postsDf.col("score")), count(postsDf.col("score"))).show();

    System.out.println("\nMaximum score per user, compared with maximum overall:");
    postsDf.filter(postsDf.col("postTypeId").equalTo(1)).select(postsDf.col("ownerUserId"), postsDf.col("acceptedAnswerId"), postsDf.col("score"),
        max(postsDf.col("score")).over(Window.partitionBy(postsDf.col("ownerUserId"))).as("maxPerUser")).
      withColumn("toMax", col("maxPerUser").minus(col("score"))).show(10);

    System.out.println("\nQuestions with their owners' next and previous questions:");
    postsDf.filter(postsDf.col("postTypeId").equalTo(1)).select(postsDf.col("ownerUserId"), postsDf.col("id"), postsDf.col("creationDate"),
        lag(col("id"), 1).over(Window.partitionBy(col("ownerUserId")).orderBy(col("creationDate"))).as("prev"),
        lead(col("id"), 1).over(Window.partitionBy(col("ownerUserId")).orderBy(col("creationDate"))).as("next")).
      orderBy(col("ownerUserId"), postsDf.col("id")).show();

    System.out.println("\nCounting tags with a UDF:");
    sqlContext.udf().register("countTags", (String tags) -> StringUtils.countMatches(tags, "&lt;"), DataTypes.IntegerType);
    Column[] cols = {postsDf.col("tags")};
    postsDf.filter(postsDf.col("postTypeId").equalTo(1)).select(postsDf.col("tags"), callUdf("countTags",
        scala.collection.JavaConversions.asScalaBuffer($(cols).toList()).seq()).as("tagCnt")).show(10, false);


      System.out.println("\n\n---- Section 5.1.4 ----");
    DataFrame cleanPosts = postsDf.na().drop();
    System.out.println("\nClean posts count:"+cleanPosts.count());
    DataFrame viewCount0 = postsDf.na().fill(ImmutableMap.of("viewCount", "0"));
    DataFrame ids1177_to_3000 = postsDf.na().replace("id acceptedAnswerId".split(" "), ImmutableMap.of(1177, 3000));
    System.out.println("\nCount of id 1177 (should be 0):"+ids1177_to_3000.select(col("id").equalTo(1177)).count());

      System.out.println("\n\n---- Section 5.1.4 ----");

    JavaRDD<Row> postsMapped = postsDf.javaRDD().map((Row row) -> RowFactory.create(row.get(0), row.get(1), row.get(2),
        row.getString(3).replace("&lt;","<").replace("&gt;",">"),
        row.get(4), row.get(5), row.get(6), row.get(7),
        row.getString(8).replace("&lt;","<").replace("&gt;",">"),
        row.get(9), row.get(10), row.get(11), row.get(12)));

    DataFrame postsDfNew = sqlContext.createDataFrame(postsMapped, postsDf.schema());

      System.out.println("\n\n---- Section 5.1.6 ----");

    postsDfNew.groupBy(postsDfNew.col("ownerUserId"), postsDfNew.col("tags"), postsDfNew.col("postTypeId")).count().orderBy(postsDfNew.col("ownerUserId").desc()).show(10);

    postsDfNew.groupBy(postsDfNew.col("ownerUserId")).agg(max(postsDfNew.col("lastActivityDate")), max(postsDfNew.col("score"))).show(10);
    Map<String, String> colFuncs = new HashMap<String, String>();
    colFuncs.put("lastActivityDate", "max");
    colFuncs.put("score", "max");
    postsDfNew.groupBy(postsDfNew.col("ownerUserId")).agg(colFuncs).show(10);

    postsDfNew.groupBy(postsDfNew.col("ownerUserId")).agg(max(postsDfNew.col("lastActivityDate")), max(postsDfNew.col("score")).gt(5)).show(10);

    DataFrame smplDf = postsDfNew.where(postsDfNew.col("ownerUserId").geq(13).and(postsDfNew.col("ownerUserId").leq(15)));
    smplDf.groupBy(smplDf.col("ownerUserId"), smplDf.col("tags"), smplDf.col("postTypeId")).count().show();

    smplDf.rollup(smplDf.col("ownerUserId"), smplDf.col("tags"), smplDf.col("postTypeId")).count().show();

    smplDf.cube(smplDf.col("ownerUserId"), smplDf.col("tags"), smplDf.col("postTypeId")).count().show();

    System.out.println("\n\n---- Section 5.1.7 ----");

    JavaRDD<String[]> itVotesRaw = sc.textFile("first-edition/ch05/italianVotes.csv").map((String x) -> x.split("~"));
    JavaRDD<Row> itVotesRows = itVotesRaw.map((String[] row) -> RowFactory.create(Long.parseLong(row[0]), Long.parseLong(row[1]), Integer.parseInt(row[2]), Timestamp.valueOf(row[3])));
    StructField[] votesFields = { DataTypes.createStructField("id", DataTypes.LongType, false),
      DataTypes.createStructField("postId", DataTypes.LongType, false),
      DataTypes.createStructField("voteTypeId", DataTypes.IntegerType, false),
      DataTypes.createStructField("creationDate", DataTypes.TimestampType, false) };
    StructType votesSchema = new StructType(votesFields);

    DataFrame votesDf = sqlContext.createDataFrame(itVotesRows, votesSchema);
    System.out.println("\nvotesDf:");
    votesDf.show();

    DataFrame postsVotes = postsDf.join(votesDf, postsDf.col("id").equalTo(votesDf.col("postId")));
    DataFrame postsVotesOuter = postsDf.join(votesDf, postsDf.col("id").equalTo(votesDf.col("postId")), "outer");

    System.out.println("\npostsVotesOuter:");
    postsVotesOuter.show();

    System.out.println("\n\n---- Section 5.1.8 ----");

    sqlContext.sql("SET spark.sql.caseSensitive=true");
    sqlContext.setConf("spark.sql.caseSensitive", "true");

    System.out.println("\n\n---- Section 5.2.1 ----");

    postsDf.registerTempTable("posts_temp");
    postsDf.write().saveAsTable("posts");
    votesDf.write().saveAsTable("votes");

    System.out.println("\n\n---- Section 5.2.2 ----");

    DataFrame resultDf = sqlContext.sql("select * from posts");

    //spark-sql> select substring(title, 0, 70) from posts where postTypeId = 1 order by creationDate desc limit 3;
    //$ spark-sql -e "select substring(title, 0, 70) from posts where postTypeId = 1 order by creationDate desc limit 3"

    System.out.println("\n\n---- Section 5.3.1 ----");

    postsDf.write().format("json").saveAsTable("postsjson");

    sqlContext.sql("select * from postsjson");

    java.util.Properties props = new java.util.Properties();
    props.setProperty("user", "user");
    props.setProperty("password", "password");
    postsDf.write().jdbc("jdbc:postgresql://postgresrv/mydb", "posts", props);

    System.out.println("\n\n---- Section 5.3.2 ----");

    //DataFrame postsDf = sqlContext.read.table("posts");
    DataFrame postsDf1 = sqlContext.table("posts");

    String[] predicates = { "viewCount > 3" };
      DataFrame result = sqlContext.read().jdbc("jdbc:postgresql://postgresrv/mydb", "posts", predicates, props);

      sqlContext.sql("CREATE TEMPORARY TABLE postsjdbc "+
        "USING org.apache.spark.sql.jdbc "+
        "OPTIONS ("+
        "url 'jdbc:postgresql://postgresrv/mydb',"+
        "dbtable 'posts',"+
        "user 'user',"+
        "password 'password')");
      sqlContext.sql("CREATE TEMPORARY TABLE postsParquet "+
        "USING org.apache.spark.sql.parquet "+
        "OPTIONS (path '/path/to/parquet_file')");
      DataFrame resParq = sqlContext.sql("select * from postsParquet");

      System.out.println("\n\n---- Section 5.4 ----");
      DataFrame postsFiltered = postsDf.filter(postsDf.col("postTypeId").equalTo(1)).withColumn("ratio", postsDf.col("viewCount").divide(postsDf.col("score"))).where(postsDf.col("ratio").lt(35));
      postsFiltered.explain(true);
  }

  public static Timestamp parseTimestamp(String s)
  {
    try { return Timestamp.valueOf(s); } catch(Exception e) { return null; }
  }
  public static Integer parseInt(String s)
  {
    try { return Integer.parseInt(s); } catch(Exception e) { return null; }
  }
  public static Long parseLong(String s)
  {
    try { return Long.parseLong(s); } catch(Exception e) { return null; }
  }

  public static Object[] parseRow(String[] split) {
    Object[] res = new Object[split.length];
    res[0] = parseInt(split[0]);
    res[1] = parseTimestamp(split[1]);
    res[2] = parseLong(split[2]);
    res[3] = split[3];
    res[4] = parseInt(split[4]);
    res[5] = parseTimestamp(split[5]);
    res[6] = parseInt(split[6]);
    res[7] = split[7];
    res[8] = split[8];
    res[9] = parseInt(split[9]);
    res[10] = parseLong(split[10]);
    res[11] = parseLong(split[11]);
    res[12] = parseLong(split[12]);
    return res;
  }

  public static class Post {
    public Post(String[] line) {
      commentCount = parseInt(line[0]);
      lastActivityDate = parseTimestamp(line[1]);
      ownerUserId = parseLong(line[2]);
      body = line[3];
      score = parseInt(line[4]);
      creationDate = parseTimestamp(line[5]);
      viewCount = parseInt(line[6]);
      title = line[7];
      tags = line[8];
      answerCount = parseInt(line[9]);
      acceptedAnswerId = parseLong(line[10]);
      postTypeId = parseLong(line[11]);
      id = parseLong(line[12]);
    }
    Integer commentCount;
    Timestamp lastActivityDate;
    Long ownerUserId;
    String body;
    Integer score;
    Timestamp creationDate;
    Integer viewCount;
    String title;
    String tags;
    Integer answerCount;
    Long acceptedAnswerId;
    Long postTypeId;
    Long id;
    public Integer getCommentCount() {
      return commentCount;
    }
    public void setCommentCount(Integer commentCount) {
      this.commentCount = commentCount;
    }
    public Timestamp getLastActivityDate() {
      return lastActivityDate;
    }
    public void setLastActivityDate(Timestamp lastActivityDate) {
      this.lastActivityDate = lastActivityDate;
    }
    public Long getOwnerUserId() {
      return ownerUserId;
    }
    public void setOwnerUserId(Long ownerUserId) {
      this.ownerUserId = ownerUserId;
    }
    public String getBody() {
      return body;
    }
    public void setBody(String body) {
      this.body = body;
    }
    public Integer getScore() {
      return score;
    }
    public void setScore(Integer score) {
      this.score = score;
    }
    public Timestamp getCreationDate() {
      return creationDate;
    }
    public void setCreationDate(Timestamp creationDate) {
      this.creationDate = creationDate;
    }
    public Integer getViewCount() {
      return viewCount;
    }
    public void setViewCount(Integer viewCount) {
      this.viewCount = viewCount;
    }
    public String getTitle() {
      return title;
    }
    public void setTitle(String title) {
      this.title = title;
    }
    public String getTags() {
      return tags;
    }
    public void setTags(String tags) {
      this.tags = tags;
    }
    public Integer getAnswerCount() {
      return answerCount;
    }
    public void setAnswerCount(Integer answerCount) {
      this.answerCount = answerCount;
    }
    public Long getAcceptedAnswerId() {
      return acceptedAnswerId;
    }
    public void setAcceptedAnswerId(Long acceptedAnswerId) {
      this.acceptedAnswerId = acceptedAnswerId;
    }
    public Long getPostTypeId() {
      return postTypeId;
    }
    public void setPostTypeId(Long postTypeId) {
      this.postTypeId = postTypeId;
    }
    public Long getId() {
      return id;
    }
    public void setId(Long id) {
      this.id = id;
    }
  }
}
