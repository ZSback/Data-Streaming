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
test_num = 1
Directory = "Data"
# change the batch interval from 1 to 10
ssc = StreamingContext(sc, 8)
ssc.checkpoint("checkpoint")


# --------------------------------------------------------------------------
# Function definition
# --------------------------------------------------------------------------
def update_cur_bar(rdd):
    c_r = rdd.collect()
    global current_barrage
    current_barrage = {}
    current_barrage = dict(c_r)


def find_real_bot(row):
    uid = row[1]
    if bot_uid:
        uid_is_bot = bot_uid.get(uid, None)
        if uid_is_bot:
            return True
    return False


def find_suspect_bot(row):
    uid = row[1]
    if bot_uid:
        uid_is_bot = bot_uid.get(uid, None)
        if uid_is_bot:
            return False
    return True


def update_bar(rdd):
    c_r = rdd.collect()
    global barrage
    for r in c_r:
        if barrage.get(r[0], 0):
            barrage[r[0]] += r[1]
        else:
            barrage[r[0]] = r[1]


def filter_level(row):
    """
    store uid_count and uid_level
    filter out high level user
    :return: True - suspect bot
    """
    level = row[2]
    if int(level) > 2:
        return False
    return True


def add_barrage_score(row):
    if row[3] in barrage:
        row[4] += barrage_score
    return row


def generate_idscore_ttbb(row):
    timestmp = row[0]
    uid = row[1]
    brg = row[3]
    score = row[4]
    key = uid
    value = [[timestmp, brg, score]]
    result = (key, value)
    return result


def score_normalizer(row):
    uid = row[0]
    t_b_s_pairs = row[1]

    scores = [int(p[2]) for p in t_b_s_pairs]
    if scores:
        max_score = str(max(scores))

    t_b_paris = [[t_b[0], t_b[1]] for t_b in t_b_s_pairs]
    result = (uid + SPACE_CHARACTER + max_score, t_b_paris)
    return result


def interval_filter(row):
    uid_score_pairs = row[0]
    t_b_pairs = row[1]

    # create the time sequence for a user
    time_seq = [datetime.datetime.strptime(t_b[0], "%Y-%m-%d %H:%M:%S") for t_b in t_b_pairs]
    time_seq.sort()

    if len(time_seq) >= 3:
        # TODO: WE CAN TUNE HERE
        start = time_seq[-3]
        end = time_seq[-1]
        if abs(end - start) < time_threshold:
            uid = uid_score_pairs.split(SPACE_CHARACTER)[0]
            score = uid_score_pairs.split(SPACE_CHARACTER)[1]

            # we add score if it is a bot
            score = int(score) + small_interval_score
            score = str(score)

            new_k = SPACE_CHARACTER.join([uid, score])
            result = (new_k, t_b_pairs)
            return result
    return row


def current_barrage_filter(row):
    uid_score_pairs = row[0]
    t_b_pairs = row[1]
    uid = uid_score_pairs.split(SPACE_CHARACTER)[0]
    score = uid_score_pairs.split(SPACE_CHARACTER)[1]

    for t_b in t_b_pairs:
        if t_b[1] in current_barrage:
            if current_barrage[t_b[1]] > barrage_threshold:
                score = int(score) - current_barrage_bonus
                if barrage.get(t_b[1], None):
                    barrage.pop(t_b[1])
                continue
        # we add score if it is a bot
        score = int(score) + current_barrage_score

    result = [uid, int(score), t_b_pairs]
    return result


def high_score(row):
    score = row[1]
    if score >= bot_score_threshold:
        return True
    return False


def update_uid_barrage(rdd):
    c_r = rdd.collect()
    global barrage
    global bot_uid
    for r in c_r:
        uid = r[0]
        bot_uid[uid] = 1
        for t_b in r[2]:
            if barrage.get(t_b[1], 0):
                barrage[t_b[1]] += r[1]
            else:
                barrage[t_b[1]] = 1


# --------------------------------------------------------------------------
# Variables initialization
# --------------------------------------------------------------------------
current_barrage = {}
barrage = {}
bot_uid = {}

low_level_score = 10
barrage_score = 10
small_interval_score = 10
current_barrage_score = 10
current_barrage_bonus = 50

time_threshold = datetime.timedelta(seconds=11)
barrage_threshold = 8
bot_score_threshold = 30

SPACE_CHARACTER = "|   |"


# --------------------------------------------------------------------------
# DStream processing
# --------------------------------------------------------------------------
textDataRDD = ssc.textFileStream(Directory)

# Get current barrage
kvPair_3 = textDataRDD.map(lambda a: (a.split(SPACE_CHARACTER)))
kvPair_3.map(lambda a: (a[3], 1)).reduceByKey(lambda a, b: a+b).foreachRDD(update_cur_bar)


# real bot and suspect bot
real_bot = kvPair_3.filter(find_real_bot)
suspect_bot = kvPair_3.filter(find_suspect_bot)
# suspect_bot.pprint()


# update barrage
bot_bar = real_bot.map(lambda a: (a[3], 1)).reduceByKey(lambda a, b: a+b).foreachRDD(update_bar)


# Filter out low level and add score
suspect_bot = suspect_bot.filter(filter_level).map(lambda a: a+[low_level_score])
# suspect_bot.pprint()


# add barrage score
suspect_bot = suspect_bot.map(add_barrage_score)
# suspect_bot.pprint()


# Map for reduceByKey
suspect_bot = suspect_bot.map(generate_idscore_ttbb).reduceByKey(lambda a, b: a+b)
# suspect_bot.pprint()


# Find the max score and reformat
group_after_normalize = suspect_bot.map(score_normalizer)
# group_after_normalize.pprint()

group_after_intervalfilter = group_after_normalize.map(interval_filter)
# group_after_intervalfilter.pprint()


group_after_curbrgfilter = group_after_intervalfilter.map(current_barrage_filter)
# group_after_curbrgfilter.pprint()

detected_bot = group_after_curbrgfilter.filter(high_score)
detected_bot.pprint()

# update bot_uid and barrage
detected_bot.foreachRDD(update_uid_barrage)



# ---------------------------------------------------------------------------------------------------------------
ssc.start()
ssc.awaitTerminationOrTimeout(48)
ssc.stop()

f = open('result.txt', 'w')
f.write(json.dumps(bot_uid, indent=2))
f.write(json.dumps(barrage, indent=2))
f.close()
