from pyspark.sql import SparkSession, SQLContext
from pyspark.streaming import StreamingContext
import pyspark
import datetime
import json
import time

# --------------------------------------------------------------------------
# DStream initialization
# --------------------------------------------------------------------------

spark = SparkSession.builder.getOrCreate()
# sc = pyspark.SparkContext("local", "1s")
sc = spark.sparkContext
Directory = "Data"
# change the batch interval from 1 to 10
ssc = StreamingContext(sc, 8)

# --------------------------------------------------------------------------
# DStream initialization
# --------------------------------------------------------------------------
PREV_COUNT = []
PREV_STATE = []

UP_RATE = 1.35
DOWN_RATE = 0.8

N = 'normal'
U = 'up'
D = 'down'

SPACE_CHARACTER = "|   |"


# --------------------------------------------------------------------------
# Function definition
# --------------------------------------------------------------------------
def compare_count_store(rdd):

    c_r = rdd.collect()

    global PREV_COUNT
    global PREV_STATE

    # print(PREV_COUNT)
    # print(PREV_STATE)

    if len(c_r) == 0:
        count_cur = 0
    else:
        count_cur = c_r[0][1]
        count_prev = PREV_COUNT[-1]
    # print(count_cur)

    #TODO: division by zero error here
    if len(PREV_COUNT) == 0:
        PREV_COUNT.append(count_cur)
        PREV_STATE.append(N)
    else:
        if UP_RATE < count_cur / PREV_COUNT[-1]:
            PREV_STATE.append(U)

        elif count_cur / PREV_COUNT[-1] < DOWN_RATE:
            PREV_STATE.append(D)

        else:
            PREV_STATE.append(N)
        PREV_COUNT.append(count_cur)



# --------------------------------------------------------------------------
# DStream processing
# --------------------------------------------------------------------------
lines = ssc.textFileStream(Directory)
pairs = lines.map(lambda s: ('1', 1))
count = pairs.reduceByKey(lambda a, b: a + b)
count.foreachRDD(compare_count_store)

danmu_count = lines.map(lambda a: ((a.split(SPACE_CHARACTER))[3], 1)).reduceByKey(lambda a, b: a + b)

# danmu_top3 = lines.foreachRDD()


# count.pprint()
# danmu_count.pprint()
# danmu_top3.pprint()
# --------------------------------------------------------------------------
ssc.start()
ssc.awaitTerminationOrTimeout(48)
ssc.stop()

print(PREV_STATE)
print(PREV_COUNT)


