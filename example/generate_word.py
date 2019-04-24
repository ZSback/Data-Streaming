import time
import datetime

INPUT_FILE_PATH = "../Data/words.txt"
OUTPUT_FILE_PATH = "../Data/"
TIME = 0.5


f = open(INPUT_FILE_PATH, "r")
if f.mode == 'r':
    contents = f.read()


count = 0
while (1):
    f = open(OUTPUT_FILE_PATH + str(count), "a+")
    if f.mode == 'a+':
        f.write(contents)
    count += 1
    file_name = OUTPUT_FILE_PATH + str(count)
    print(f"create file is {file_name}, created time is {datetime.datetime.now()}")
    time.sleep(TIME)

