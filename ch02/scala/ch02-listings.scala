//################ Listings from chapter 02 ################\\
//### 2.1

/************************************ terminal
which javac

sudo apt-get update
sudo apt-get -y install openjdk-7-jdk

//### 2.2
cd $HOME/Downloads
tar -xvf spark-1.3.0-bin-hadoop2.4.tgz

echo $HOME

cd $HOME
mkdir –p bin/sparks
mv Downloads/spark-1.3.0-bin-hadoop2.4 bin/sparks

cd $HOME/bin
ln -s spark-1.3.0-bin-hadoop2.4 Spark

//# Example of how to replace symbolic link
//# to point to a different version of Spark:
//# rm Spark
//# ln -s spark-1.2.1-bin-hadoop2.4 Spark

//### 2.3
cd $HOME/bin/Spark
./bin/spark-shell

gedit conf/log4j.properties
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
./bin/spark-shell
end terminal ************************************/


//### 2.3.1
val licLines = sc.textFile("LICENSE")
val lineCnt = licLines.count

val bsdLines = licLines.filter(line => line.contains("BSD"))
bsdLines.count

bsdLines.foreach(bLine => println(bLine))

//### 2.5.1
val nums = sc.parallelize(1 to 10)
val numsEven = nums.filter(num => num % 2 == 0)
numsEven.foreach(x => println(x))

def isEven(num: Int) = { num % 2 == 0 }
val isEvenInVal = (num: Int) => num % 2 == 0
val numsEven1 = nums.filter(isEvenInVal)
numsEven1.count

//### 2.5.2
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

//### 2.5.3
//# def distinct(): RDD[T]
//# def distinct(numPartitions: Int): RDD[T]

/************************************ terminal:
cd ~/Spark
echo "15,16,20,20,68,77,80,94,94,98
16,31,31,48,55,55,71,77,85,85,90
2,11,21,21,36,38,55,71,77,85,94
8,12,20,20,20,31,50,68
4,8,9,10,20,21,21,36,42,61,94,98,98,98
28,31,46,48,71,71,71,77,77,90,98
16,21,21,21,61,68,71,77,80,94" > client-ids.log
end terminal ********************************/

val lines = sc.textFile("client-ids.log")

val idsStr = lines.map(line => line.split(","))
idsStr.foreach(println(_))

strIds.first

idsStr.collect

//# def flatMap[U](f: (T) => TraversableOnce[U]): RDD[U]

val ids = lines.flatMap(_.split(","))

ids.collect

ids.first

ids.collect.mkString("; ")

val intIds = ids.map(_.toInt)
intIds.collect

val uniqueIds = intIds.distinct
uniqueIds.collect
val countForGrumpy = uniqueIds.count

val transactionCount = ids.count

//### 2.5.4
//# def sample(withReplacement: Boolean, fraction: Double, seed: Long = Utils.random.nextLong): RDD[T]

val lines = sc.textFile("client-ids.log")
val ids = lines.flatMap(_.split(","))
ids.count
val uniqueIds = ids.distinct
uniqueIds.count
uniqueIds.collect

val s = uniqueIds.sample(false, 0.07)
s.count
s.collect

val swr = uniqueIds.sample(true, 0.5)
swr.count
swr.collect

//# def takeSample(withReplacement: Boolean, num: Int, seed: Long = Utils.random.nextLong): Array[T]

val taken = uniqueIds.takeSample(false, 5)
swr.take(6)
swr.min
swr.max

