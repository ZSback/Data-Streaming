# 这个抓取弹幕后保存为text文档，然后词云分析,此部分是抓取弹幕内容
__author__ = '布咯咯_rieuse'
__time__ = '2017.6.2'
__github__ = 'https://github.com/rieuse'

import multiprocessing
import re
import socket
import time
import datetime

import requests
from bs4 import BeautifulSoup

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
port = 8601
client.connect(('119.96.201.28', port))

danmu_path = re.compile(b'txt@=(.+?)/cid@')
uid_path = re.compile(b'uid@=(.+?)/nn@')
nickname_path = re.compile(b'nn@=(.+?)/txt@')
level_path = re.compile(b'.*?/level@=(.*?)/')
# pattern = re.compile(b'type@=chatmsg/.+?/nn@=(.+?)/txt@=(.+?)/.+?/level@=(.+?)/')


def sendmsg(msgstr):
    msg = msgstr.encode('utf-8')
    data_length = len(msg) + 8
    code = 689
    msgHead = int.to_bytes(data_length, 4, 'little') \
              + int.to_bytes(data_length, 4, 'little') + int.to_bytes(code, 4, 'little')
    client.send(msgHead)
    sent = 0
    while sent < len(msg):
        tn = client.send(msg[sent:])
        sent = sent + tn


def start(roomid):
    # login request
    msg = 'type@=loginreq/username@=rieuse/password@=douyu/roomid@={}/\0'.format(roomid)
    sendmsg(msg)
    # get barrages
    msg_more = 'type@=joingroup/rid@={}/gid@=-9999/\0'.format(roomid)
    sendmsg(msg_more)

    # print('---------------欢迎连接到{}的直播间---------------'.format(get_name(roomid)))
    print('---------------欢迎连接到某一个直播间---------------')
    while True:
        data = client.recv(1024)
        print('-' * 100)
        uid_more = uid_path.findall(data)
        level_more = level_path.findall(data)
        danmu_more = danmu_path.findall(data)
        if not data:
            break
        else:
            for i in range(0, len(danmu_more)):
                with open('danmutxt2', 'a') as fo:
                    try:
                        danmu = danmu_more[i].decode(encoding='utf-8')
                        uid = uid_more[i].decode(encoding='utf-8')
                        level = level_more[i].decode(encoding='utf-8')
                        print("danmu: " + danmu)
                        print("user id: " + uid)
                        print("level: " + level + "\n")

                        # write to files
                        txt = str(datetime.datetime.now()) + "|   |"
                        txt += danmu + "|   |"
                        txt += uid + "|   |"
                        txt += level + '\n'
                        fo.writelines(txt)

                    except Exception as e:
                        print(e)
                        print('出错了\n')


def keeplive():
    while True:
        msg = 'type@=keeplive/tick@=' + str(int(time.time())) + '/\0'
        sendmsg(msg)
        time.sleep(10)


def get_name(roomid):
    r = requests.get("http://www.douyu.com/" + roomid)
    soup = BeautifulSoup(r.text, 'lxml')
    return soup.find('a', {'class', 'zb-name'}).string


if __name__ == '__main__':
    room_id = input('请输入房间ID： ')
    # room_id = 1756497
    p1 = multiprocessing.Process(target=start, args=(room_id,))
    p2 = multiprocessing.Process(target=keeplive)
    p1.start()
    p2.start()
