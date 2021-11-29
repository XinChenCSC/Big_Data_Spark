
import sys
import csv
import datetime
import json
import pyspark
import numpy as np
from pyspark.sql import SparkSession
from pyspark import SparkContext
sc = pyspark.SparkContext()
spark = SparkSession(sc)

def get_info(target):
  categorie = sc.textFile('hdfs:///data/share/bdm/core-places-nyc.csv')\
                .filter(lambda x: next(csv.reader([x]))[9] in target)\
                .map(lambda x: next(csv.reader([x])))\
                .map(lambda x: (x[0],x[1]))\
                .cache()
  return categorie
def get_visit_data(target):
  visit_data = sc.textFile('hdfs:///data/share/bdm/weekly-patterns-nyc-2019-2020/*')\
                   .filter(lambda x: tuple(next(csv.reader([x]))[0:2]) in  target)\
                   .map(lambda x: next(csv.reader([x])))\
                   .map(lambda x: (x[12].split('T')[0],x[13].split('T')[0],x[16]))\
                   .cache()
  return visit_data

def visit_per_day(visit_data):
  
  from datetime import date
  d = visit_data[0].split('-')
  d0 = date(int(d[0]),int(d[1]),int(d[2]))
  d = visit_data[1].split('-')
  d1 = date(int(d[0]),int(d[1]),int(d[2]))
  diff = d1 - d0
  delta = datetime.timedelta(days=1)
  visit = visit_data[2].replace('[','').replace(']','')
  for x in visit.split(','):
    yield (str(d0),int(x))
    d0+= delta

def count_low(x):
  low = int(np.median(x[1])) - int(np.std(x[1]))
  if low < 0:
    return 0
  else:
    return low

def count_high(x):
  high = int(np.median(x[1])) + int(np.std(x[1]))
  if high < 0:
    return 0
  else:
    return high
  
def get_res_table(target):
  res = target.filter(lambda x:x[1] != 0 and x[0] >'2018-12-31' and x[0] < '2021-01-01')\
                                             .groupByKey()\
                                             .mapValues(list)\
                                             .sortBy(lambda x: x[0])\
                                             .map(lambda x: (x[0].split('-')[0],x[0].replace('2019','2020'),int(np.median(x[1])),count_low(x),count_high(x)))
  return res
if __name__=='__main__':
  categories = [set(['452210','452311']),set(['445120']),set(['722410']),set(['722511']),
              set(['722513']), set(['446110','446191']),set(['311811','722515']),
                set( ['445210','445220','445230','445291','445292','445299']), 
                set(['445110'])]

  path_name = ('test/big_box_grocers',
            'test/convenience_stores',
            'test/drinking_places',
            'test/full_service_restaurants',
            'test/limited_service_restaurants',
            'test/pharmacies_and_drug_stores',
            'test/snack_and_bakeries',
            'test/specialty_food_stores',
            'test/supermarkets_except_convenience_stores')
  for index,categorie in enumerate(categories):
    get_res_table(get_visit_data(get_info(categorie)\
    .collect())\
    .flatMap(visit_per_day))\
    .map(lambda x: (x[0], x[1] , x[2],x[3],x[4]))\
    .toDF(["year", "date" , "median","low","high"])\
    .coalesce(1)\
    .write.format("csv")\
    .option("header", "true")\
    .save(path_name[index])
  
