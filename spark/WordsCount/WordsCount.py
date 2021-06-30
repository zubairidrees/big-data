from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext

conf =SparkConf().setAppName("Streaming work count")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc,15)
lines = ssc.socketTextStream("localhost",9999)

words = lines.flatMap(lambda x: x.split(" "))
wordsTuples = words.map(lambda w:(w,1))
wordsCount = wordsTuples.reduceByKey(lambda x,y:x+y)

wordsCount.saveAsTextFiles("words_count","txt")

ssc.start()
ssc.awaitTermination()
