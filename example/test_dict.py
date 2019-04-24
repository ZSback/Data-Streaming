#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

r"""
 Counts words in UTF8 encoded, '\n' delimited text received from the
 network every second.

 Usage: stateful_network_wordcount.py <hostname> <port>
   <hostname> and <port> describe the TCP server that Spark Streaming
    would connect to receive data.

 To run this on your local machine, you need to first run a Netcat server
    `$ nc -lk 9999`
 and then run the example
    `$ bin/spark-submit examples/src/main/python/streaming/stateful_network_wordcount.py \
        localhost 9999`
"""
from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


def getWordBlacklist(sparkContext):
    if ('wordBlacklist' not in globals()):
        globals()['wordBlacklist'] = sparkContext.broadcast(["a", "b", "c"])
    return globals()['wordBlacklist']

if __name__ == "__main__":

    a = []
    Directory = "../Data"

    sc = SparkContext(appName="PythonStreamingStatefulNetworkWordCount")
    ssc = StreamingContext(sc, 1)


    def print_dic(row):
        print('-' * 20)
        print(a)
        print('-' * 20)
        return row

    def collect_rdd(rdd):
        c_r = rdd.collect()
        a.append(c_r)

    def test(row):
        return row

    def combine(v1, v2):
        return

    lines = ssc.textFileStream(Directory)

    result = lines.map(test)
    result.foreachRDD(collect_rdd)
    result = result.map(test)
    result2 = lines.map(print_dic)

    result.pprint()
    result2.pprint()



    ssc.start()
    ssc.awaitTermination()

    print(a)
