# true people data generate
from threading import Thread
import uuid
import random
import string
import datetime
import time

SPLIT_CHAR = "|   |"
USER_LIST = ['刘三藏', '张全蛋', '隔壁老王', '深海鱼biss',  '火星包']
TEXT_LIST = ['暗示洗澡？',  '真实4.4', '这把 我上 我也行     我黑铁的  这把 我上 我也行', '主播不要逗']

# bot changeable
two_bot_interval = 2
bot_interval = 0.3

def generate_realuser_chat():
    print("USER IS RUNNING")
    with open('danmutxt2.txt', 'a+') as fo:
        while True:
            timestamp = datetime.datetime.now()
            random_uid = uuid.uuid4()
            random_level = random.randint(1,50)
            random_text =  ''.join([random.choice(string.ascii_letters + string.digits) for n in range(32)])
            result = f"{timestamp}{SPLIT_CHAR}{random_uid}{SPLIT_CHAR}{random_level}{SPLIT_CHAR}{random_text}\n"

            time.sleep(0.3)
            fo.writelines(result)


generate_realuser_chat()


