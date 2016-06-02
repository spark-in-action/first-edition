//################ Listings from chapter 02 ################\\

/************************************ terminal

# section 2.1
vagrant up

vagrant ssh

# section 2.1.1
which java
ls -la /usr/bin/java
ls -la /etc/alternatives/java
echo $JAVA_HOME

# section 2.1.2
hadoop fs -ls /user
# /usr/local/hadoop/sbin/start-dfs.sh
# /usr/local/hadoop/sbin/stop-dfs.sh

# section 2.1.3
ls /opt | grep spark

# Example of how to replace symbolic link
# to point to a different version of Spark:
# sudo rm -f /usr/local/spark
# sudo ln -s /opt/spark-1.5.0-bin-hadoop2.4 /usr/local/spark

export | grep SPARK

//section 2.2.1
spark-shell

nano /usr/local/spark/conf/log4j.properties
end terminal ********************************/


/************************************ log4j.properties
# set global logging severity to INFO (and upwards: WARN, ERROR, FATAL)
log4j.rootCategory=INFO, console, file

# console config (restrict only to ERROR and FATAL)
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.err
log4j.appender.console.threshold=ERROR
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n

# file config
log4j.appender.file=org.apache.log4j.RollingFileAppender
log4j.appender.file.File=logs/info.log
log4j.appender.file.MaxFileSize=5MB
log4j.appender.file.MaxBackupIndex=10
log4j.appender.file.layout=org.apache.log4j.PatternLayout
log4j.appender.file.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n

# Settings to quiet third party logs that are too verbose
log4j.logger.org.eclipse.jetty=WARN
log4j.logger.org.eclipse.jetty.util.component.AbstractLifeCycle=ERROR
log4j.logger.org.apache.spark.repl.SparkIMain$exprTyper=INFO
log4j.logger.org.apache.spark.repl.SparkILoop$SparkILoopInterpreter=INFO
end log4j.properties ********************************/


/**************************************** terminal
spark-shell
end terminal ************************************/


//### 2.2.2
val licLines = sc.textFile("/usr/local/spark/LICENSE")
val lineCnt = licLines.count

val bsdLines = licLines.filter(line => line.contains("BSD"))
bsdLines.count

def isBSD(line: String) = { line.contains("BSD") }
val isBSD = (line: String) => line.contains("BSD")
val bsdLines1 = licLines.filter(isBSD)
bsdLines1.count
bsdLines.foreach(bLine => println(bLine))

//### 2.3.1
//# def map[U](f: (T) => U): RDD[U]

val numbers = sc.parallelize(10 to 50 by 10)
numbers.foreach(x => println(x))
val numbersSquared = numbers.map(num => num * num)
numbersSquared.foreach(x => println(x))

val reversed = numbersSquared.map(x => x.toString.reverse)
reversed.foreach(x => println(x))

val alsoReversed = numbersSquared.map(_.toString.reverse)
alsoReversed.first
alsoReversed.top(4)

//### 2.3.2

/************************************ terminal:
echo "15,16,20,20
77,80,94
94,98,16,31
31,15,20" > ~/client-ids.log
end terminal ********************************/

val lines = sc.textFile("/home/spark/client-ids.log")

val idsStr = lines.map(line => line.split(","))
idsStr.foreach(println(_))

idsStr.first

idsStr.collect

//# def flatMap[U](f: (T) => TraversableOnce[U]): RDD[U]

val ids = lines.flatMap(_.split(","))

ids.collect

ids.first

ids.collect.mkString("; ")

val intIds = ids.map(_.toInt)
intIds.collect

//# def distinct(): RDD[T]

val uniqueIds = intIds.distinct
uniqueIds.collect
val finalCount  = uniqueIds.count

val transactionCount = ids.count

//Pasting blocks of code
scala> val lines = sc.textFile("/home/spark/client-ids.log")
lines: org.apache.spark.rdd.RDD[String] = client-ids.log MapPartitionsRDD[12] at textFile at <console>:21
scala> val ids = lines.flatMap(_.split(","))
ids: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[13] at flatMap at <console>:23
scala> ids.count
res8: Long = 14
scala> val uniqueIds = ids.distinct
uniqueIds: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[16] at distinct at <console>:25
scala> uniqueIds.count
res17: Long = 8
scala> uniqueIds.collect
res18: Array[String] = Array(16, 80, 98, 20, 94, 15, 77, 31)

//### 2.3.3
//# def sample(withReplacement: Boolean, fraction: Double, seed: Long = Utils.random.nextLong): RDD[T]

val s = uniqueIds.sample(false, 0.3)
s.count
s.collect

val swr = uniqueIds.sample(true, 0.5)
swr.count
swr.collect

//# def takeSample(withReplacement: Boolean, num: Int, seed: Long = Utils.random.nextLong): Array[T]

val taken = uniqueIds.takeSample(false, 5)
uniqueIds.take(3)

//### 2.4

//Implicit conversion:
class ClassOne[T](val input: T) { }
class ClassOneStr(val one: ClassOne[String]) {
    def duplicatedString() = one.input + one.input
}
class ClassOneInt(val one: ClassOne[Int]) {
    def duplicatedInt() = one.input.toString + one.input.toString
}
implicit def toStrMethods(one: ClassOne[String]) = new ClassOneStr(one)
implicit def toIntMethods(one: ClassOne[Int]) = new ClassOneInt(one)

scala> val oneStrTest = new ClassOne("test")
oneStrTest: ClassOne[String] = ClassOne@516a4aef
scala> val oneIntTest = new ClassOne(123)
oneIntTest: ClassOne[Int] = ClassOne@f8caa36
scala> oneStrTest.duplicatedString()
res0: String = testtest
scala> oneIntTest.duplicatedInt()
res1: 123123

//### 2.4.1
intIds.mean
intIds.sum

intIds.variance
intIds.stdev

//### 2.4.2
intIds.histogram(Array(1.0, 50.0, 100.0))
intIds.histogram(3)

//# def sumApprox(timeout: Long, confidence: Double = 0.95): PartialResult[BoundedDouble]
//# def meanApprox(timeout: Long, confidence: Double = 0.95): PartialResult[BoundedDouble]

