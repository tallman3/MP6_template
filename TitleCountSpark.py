#!/usr/bin/env python

'''Exectuion Command: spark-submit TitleCountSpark.py stopwords.txt delimiters.txt dataset/titles/ dataset/output'''

import sys
from pyspark import SparkConf, SparkContext

stopWordsPath = sys.argv[1]
delimitersPath = sys.argv[2]
stopWords = []
delims = []
with open(stopWordsPath) as f:
	#TODO
    stopWords  = f.read().splitlines()

with open(delimitersPath) as f:
    #TODO
    delims= list(f.read())

def splitLine(line):
    goodWords = []
    for delim in delims:
        line = line.replace(delim," ")
    words = line.split()
    for word in words:
        if word.lower() not in stopWords:
            goodWords.append((word.lower(), 1))
    return goodWords

conf = SparkConf().setMaster("local").setAppName("TitleCount")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf = conf)

lines = sc.textFile(sys.argv[3],1)

#TODO
mapLines = lines.flatMap(splitLine)
counts = mapLines.reduceByKey( lambda a, b: a + b)
topLines = counts.sortBy(lambda a: a[1], ascending=False).take(10)
outputLines = sorted(topLines)
outputFile = open(sys.argv[4],"w", errors='ignore')
for aLine in outputLines:
    outputFile.write("{}\t{}\n".format(aLine[0], aLine[1]))

#TODO
#write results to output file. Foramt for each line: (line +"\n")

sc.stop()
