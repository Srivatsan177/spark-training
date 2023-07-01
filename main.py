from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("master").setMaster("local")
sc = SparkContext(conf=conf)
data = sc.parallelize("README.md")
print(data)
