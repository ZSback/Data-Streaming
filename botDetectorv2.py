# Part 3
# !/usr/bin/env python
# coding: utf-8
from pyspark.sql import SparkSession, SQLContext
from pyspark.streaming import StreamingContext
import pyspark
import datetime
import json
import time


# TODO: we should tune the time
time_threshhold = datetime.timedelta(seconds=11)
barrage_threshold = 5
'''
barrage = {"barrage#1": '# of barrage#1'
           }
'''
barrage = {}  # do not clear in new loop, dict #1
'''
barrage = {"uid#1": True
           }
'''
bot_uid = {}  # do not clear in new loop, dict #2

current_barrage = {}

uid_info_1 = {}  # clear in every new loop
uid_info_2 = {}  # clear in every new loop
'''
uid_info_1 = {"8977601":
                {  "count":2,
                   "level":12
                },
            "6544213":
                {
                   ...
                },
                 ...
            }
'''
'''
uid_info_2 = {"8977601":
                {  "time":["12:21:31", "12:21:59"],
                   "barrage":["barrage #1", "barrage #2"]
                },
            "6544213":
                {
                   ...
                },
                 ...
            }
'''


def filter_begin(row):
    uid_info_1 = {}
    current_barrage = {}

    uid = row[1]
    brg = row[3]  # barrage
    # get current barrage
    # Exception: true audience may send two meaningless barrage in a short time by mistake and then send a 6666
    # before a 666 mass, it will be considered as a bot.
    if current_barrage.get(brg, None):
        current_barrage[brg] += 1
    else:
        current_barrage[brg] = 1

    if bot_uid:
        uid_is_bot = bot_uid.get(uid, None)
        if uid_is_bot:
            # if it is a bot, it will not be suspect. So we don't need further filter.
            if barrage.get(brg, None):
                barrage[brg] += 1
            else:
                barrage[brg] = 1
            return False
    return True


def filter_level(row):
    """
    store uid_count and uid_level
    filter out high level user
    :return: True - suspect bot
    """
    uid = row[1]
    level = row[2]

    if uid_info_1[uid].get("count", None):
        uid_info_1[uid]["count"] += 1
    else:
        uid_info_1[uid]["count"] = 1

    if not uid_info_1[uid].get("level", None):
        uid_info_1[uid]["level"] = level

    if level > 2:
        return False
    return True


def store_brg_time(row):
    time = row[0]
    uid = row[1]
    brg = row[3]  # barrage
    if not uid_info_2[uid].get("time", None):
        uid_info_2[uid]["time"] = [time]
    else:
        if len(uid_info_2[uid]["time"]) >= 3:
            uid_info_2[uid]["time"].remove(uid_info_2[uid]["time"][0])
            uid_info_2[uid]["time"].append(time)
        else:
            uid_info_2[uid]["time"].append(time)

    if not uid_info_2[uid].get("barrage", None):
        uid_info_2[uid]["barrage"] = [brg]
    else:
        if len(uid_info_2[uid]["barrage"]) >= 3:
            uid_info_2[uid]["barrage"].remove(uid_info_2[uid]["barrage"][0])
            uid_info_2[uid]["barrage"].append(brg)
        else:
            uid_info_2[uid]["barrage"].append(brg)
    return row


def filter_high_frequency(row):
    """
    :return: True - suspect bot
    """
    time = row[0]
    uid = row[1]
    brg = row[3]  # barrage
    if len(uid_info_2[uid]["time"]) >= 3:
        # TODO: bug here, the fourth uid comes then filter, when the third comes, determine if there is two, if there are two then we should
        #        get the interval and determine if it is high frequency.
        # convert to datetime
        end = datetime.datetime.strptime(uid_info_2[uid]["time"][2], "%Y-%m-%d %H:%M:%S")
        begin = datetime.datetime.strptime(uid_info_2[uid]["time"][0], "%Y-%m-%d %H:%M:%S")
        interval = end - begin
        if interval < time_threshhold:
            return True
    return False


def store_suspect_brg(row):
    """
    :return: those who is not bot
    """
    # TODO: don't understand here.
    uid = row[1]
    # barrage  = {}    do not clear in new loop, dict #1
    for brg in uid_info_2[uid]["barrage"]:
        if barrage.get(brg, None):
            barrage[brg] += 1
        else:
            barrage[brg] = 1
        if barrage[brg] > 1:
            if current_barrage[brg] < barrage_threshold:
                # this means that the user is a bot
                update_info_bot(row)
            else:
                return row


def update_info_bot(row):
    # according to the decision above, add new bot or not.
    # TODO: here it does not make sense.
    uid = row[1]
    if bot_uid.get(uid, None):
        bot_uid[uid] += 1
    else:
        bot_uid[uid] = 1


# --------------------------------------------------------------------------
spark = SparkSession.builder.getOrCreate()
# sc = pyspark.SparkContext("local", "1s")
sc = spark
test_num = 1
Directory = "Data"
# change the batch interval from 1 to 10
ssc = StreamingContext(sc, 10)
# --------------------------------------------------------------------------



# TODO: here the code is wrong.
# op_fn = file_name + str(test_num) + ".txt"
textDataRDD = ssc.textFileStream(Directory)

kvPair_3 = textDataRDD.map(lambda a:(a.split("|   |")))
# kvPair_3 = kvPair_3.filter(lambda a: a[-1].isdigit())
kvPair_3 = kvPair_3.filter(filter_begin)
kvPair_3 = kvPair_3.filter(filter_level)
kvPair_3 = kvPair_3.map(store_brg_time)
kvPair_3 = kvPair_3.filter(filter_high_frequency)
kvPair_3 = kvPair_3.map(store_suspect_brg)

# kvPair_3.collect()
kvPair_3.pprint()

ssc.start()
ssc.awaitTerminationOrTimeout(60)
ssc.stop()


f = open("result.txt",'a')
f.write(json.dumps(barrage, intent = 2))
f.write(json.dumps(bot_uid, intent = 2))
