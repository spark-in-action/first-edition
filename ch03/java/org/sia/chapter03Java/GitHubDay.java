package org.sia.chapter03Java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.DataFrame;
import java.util.List;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;

public class GitHubDay {

	public static void main(String[] args) throws IOException {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf());
	    SQLContext sqlContext = new SQLContext(sc);
	    
	    DataFrame ghLog = sqlContext.read().json(args[0]);
	   
	    DataFrame pushes = ghLog.filter("type = 'PushEvent'");
	    DataFrame grouped = pushes.groupBy("actor.login").count();
	    DataFrame ordered = grouped.orderBy(grouped.col("count").desc());
	    
	    // Broadcast the employees set
	    List<String> employees = Files.readAllLines(new File(args[1]).toPath(), Charset.defaultCharset() );
	    
	    Broadcast<List<String>> bcEmployees = sc.broadcast(employees);
	    
	    sqlContext.udf().register("SetContainsUdf", (String user) -> bcEmployees.value().contains(user), DataTypes.BooleanType);
	    DataFrame filtered = ordered.filter("SetContainsUdf(login)");
	    
	    filtered.write().format(args[3]).save(args[2]);
	}
}
