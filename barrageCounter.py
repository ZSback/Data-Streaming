from pyspark.sql import SparkSession, SQLContext
from pyspark.streaming import StreamingContext
import pyspark
import datetime
import json

f = open('result.txt', 'w')
f.write(" ")
f.close()


# --------------------------------------------------------------------------
# Variables initialization
# --------------------------------------------------------------------------
current_barrage = {}
barrage = {}
bot_uid = {}

windowsize = 8
count = []
increase_rate = []
getHeatThreshold = 0.35
loseHeatThreshold = -0.2

# --------------------------------------------------------------------------
# DStream initialization
# --------------------------------------------------------------------------
spark = SparkSession.builder.getOrCreate()
# sc = pyspark.SparkContext("local", "1s")
sc = spark.sparkContext
test_num = 1
Directory = "Data"
# change the batch interval from 1 to 10
ssc = StreamingContext(sc, windowsize)
ssc.checkpoint("checkpoint")


def update_count(rdd):
    c_r = rdd.collect()
    print("------------update_count---------------")
    print(c_r)
    print("---------------------------------------")
    global count, increase_rate
    label = ""
    if c_r:
        count.append(c_r[0][1])
        if len(count) > 1:
            rate = (c_r[0][1] - count[-2]) / count[-2]
            increase_rate.append(rate)
            if rate > getHeatThreshold:
                label = "Heat begin.\n\n"
            elif (rate+increase_rate[-2]) < loseHeatThreshold and increase_rate[-2] < getHeatThreshold:
                label = "Heat end.\n\n"
            else:
                label = "Keeping.\n\n"
        else:
            increase_rate.append(0)
            label = "Start.\n\n"

        ff = open('result.txt', 'a')
        ff.write(str(count)+'\n')
        ff.write(str(increase_rate)+'\n')
        ff.write(label)
        ff.close()
    print("------------update_count---------------")
    print(count)
    print("---------------------------------------")


# --------------------------------------------------------------------------
# DStream processing
# --------------------------------------------------------------------------
textDataRDD = ssc.textFileStream(Directory)
count_barrage = textDataRDD.map(lambda a: ("barrage", 1)).reduceByKey(lambda a, b: a+b).foreachRDD(update_count)
# print(count)


# --------------------------------------------------------------------------
# DStream main()
# --------------------------------------------------------------------------
ssc.start()
ssc.awaitTerminationOrTimeout(64)
ssc.stop()

print("DStream finished.")
f = open('result.txt', 'a')
f.write(str(count)+'\n\n')
f.close()
