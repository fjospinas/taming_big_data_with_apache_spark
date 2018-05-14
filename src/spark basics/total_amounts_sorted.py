import os
from pyspark import SparkContext, SparkConf
from pathlib import Path


def parse_line(x):
    fields = x.split(',')
    id = fields[0]
    amount = float(fields[2])
    return id, amount


data_path = os.path.join(str(Path(__file__).parents[2]), 'data', 'customer-orders.csv')

conf = SparkConf().setAppName('TotalAmountsApp')
sc = SparkContext(conf=conf)

text_file = sc.textFile(data_path)

all_build = text_file.map(lambda x: parse_line(x))

sums = all_build.reduceByKey(lambda a, b: a + b)\

sums = sums.map(lambda x: (x[1], x[0])).sortByKey().collect()

for tup in sums:
    print(tup[1], '%.2f' % tup[0])
