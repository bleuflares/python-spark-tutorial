import re
import sys
from pyspark import SparkConf, SparkContext

ALPHABET_PAIRS = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']

def select_alphabet(s):
	if len(s) > 0:
		if s[0] in ALPHABET_PAIRS:
			return True

conf = SparkConf()
sc = SparkContext(conf=conf)
lines = sc.textFile(sys.argv[1])
words = lines.flatMap(lambda l: re.split(r'[^\w]+', l))
lower_words = words.map(lambda w: w.lower())
unique_words = lower_words.distinct()
alphabet_words = unique_words.filter(select_alphabet)
pairs = alphabet_words.map(lambda aw: (aw[0], 1))
counts = pairs.reduceByKey(lambda n1, n2: n1 + n2)
counts.saveAsTextFile(sys.argv[2])
sc.stop()