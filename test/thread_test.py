from threading import Thread
import time

def t1():
    while True:
        print ('thread 1')
        time.sleep(1)
        print ('juan')
        print ('thread one')


def t2():
    while True:
        print ('thread 2')
        print ('yew')
        print ('thread two')



one = Thread(target=t1)
one.start()

two = Thread(target=t2)
two.start()


