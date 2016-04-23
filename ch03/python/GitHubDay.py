from __future__ import print_function
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import sys

if __name__ == "__main__":
	sc   = SparkContext(conf=SparkConf())
	sqlContext = SQLContext(sc)
	ghLog = sqlContext.read.json(sys.argv[1])

	pushes = ghLog.filter("type = 'PushEvent'")
	grouped = pushes.groupBy("actor.login").count()
	ordered = grouped.orderBy(grouped['count'], ascending=False)

	# Broadcast the employees set	
	employees = [line.rstrip('\n') for line in open(sys.argv[2])]

	bcEmployees = sc.broadcast(employees)

	def isEmp(user):
		return user in bcEmployees.value

	from pyspark.sql.types import BooleanType
	sqlContext.udf.register("SetContainsUdf", isEmp, returnType=BooleanType())
	filtered = ordered.filter("SetContainsUdf(login)")

	filtered.write.format(sys.argv[4]).save(sys.argv[3])
