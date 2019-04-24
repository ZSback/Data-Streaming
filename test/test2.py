# true people data generate
from threading import Thread
import uuid
import random
import string
import datetime
import time

SPLIT_CHAR = "|   |"
USER_LIST = ['刘三藏', '张全蛋', '隔壁老王', '深海鱼biss',  '火星包']
TEXT_LIST = ['666']

# bot changeable
two_bot_interval = 2
bot_interval = 0.3


def generate_chat(fo):
    print(f"Generate TETS_CASE2 fake  barrage info to {fo}")

    with open(fo, 'a+') as fo:
        # generate random number of real users

        for _ in range(random.randint(1, 80)):

            timestamp = str(datetime.datetime.now()).split('.')[0]
            random_uid = uuid.uuid4()
            random_level = random.randint(1,50)
            random_text =  '666'

            result = f"{timestamp}{SPLIT_CHAR}{random_uid}{SPLIT_CHAR}{random_level}{SPLIT_CHAR}{random_text}\n"

            fo.writelines(result)
            time.sleep(0.05)

        # generate a bot user
        random_bot_uid = USER_LIST[random.randint(0, len(USER_LIST) - 1)]
        bot_level = 1
        for _ in range(3):
            timestamp = str(datetime.datetime.now()).split('.')[0]
            bot_text = TEXT_LIST[random.randint(0, len(TEXT_LIST) - 1)]

            result = f"{timestamp}{SPLIT_CHAR}{random_bot_uid}{SPLIT_CHAR}{bot_level}{SPLIT_CHAR}{bot_text}\n"

            fo.writelines(result)
            time.sleep(0.05)


# generate test file
count = 0
while True:
    generate_chat(f'../Data/test{count}.txt')

    count += 1