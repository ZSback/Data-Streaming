import jieba
from pyspark.sql import SparkSession, SQLContext
from pyspark.streaming import StreamingContext
import pyspark
import datetime
import json

f = open('result.txt', 'w')
f.write(" ")
f.close()
windowsize = 8
SPACE_CHARACTER = "|   |"
accumulate_barrage = {}

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


def splitWord(row):
    seg_list = jieba.cut(row, cut_all=False)
    seg = list(seg_list)
    # print("Default Mode: " + "/ ".join(seg))  # 精确模式
    return seg


def topBarrage(rdd):
    collect = rdd.collect()
    word_count_dict = dict(collect)
    for k in word_count_dict:
        if k != " ":
            if accumulate_barrage.get(k, None):
                accumulate_barrage[k] += word_count_dict[k]
            else:
                accumulate_barrage[k] = word_count_dict[k]
    # word_count_dict_tuple = sorted(word_count_dict)
    word_count_dict_tuple = sorted(word_count_dict.items(), key=lambda kv: kv[1], reverse=True)
    for i in range(3):
        print("   "+str(word_count_dict_tuple[i]))


# --------------------------------------------------------------------------
# DStream processing
# --------------------------------------------------------------------------
textDataRDD = ssc.textFileStream(Directory)
textDataRDD = textDataRDD.map(lambda row: row.split(SPACE_CHARACTER)[3])
count_barrage = textDataRDD.flatMap(splitWord).map(lambda word: (word, 1))
count_word = count_barrage.reduceByKey(lambda a, b: a+b)
count_word.foreachRDD(topBarrage)
# print(count)


# --------------------------------------------------------------------------
# DStream main()
# --------------------------------------------------------------------------
ssc.start()
ssc.awaitTerminationOrTimeout(64)
ssc.stop()

print("DStream finished.")
f = open('result.txt', 'a')
f.write("result:")
f.write(str(sorted(accumulate_barrage.items(), key=lambda kv: kv[1], reverse=True)[:10]))
f.close()
