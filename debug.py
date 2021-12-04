
import sys
import csv
import datetime
import pyspark
import numpy as np
from pyspark.sql import SparkSession
from pyspark import SparkContext
from datetime import date
import itertools



def get_name_id(x):
    for data in categories:
      if x[9] in data:
        return [x[0],x[1],data[-1]]

def get_pattern_info(x):
  if x[0] in data:
    return (x[12].split('T')[0],x[13].split('T')[0],x[16],x[0],x[1])

def visit_per_day(visit_data):
  if visit_data is not None:
    d = visit_data[0].split('-')
    d0 = date(int(d[0]),int(d[1]),int(d[2]))
    d = visit_data[1].split('-')
    d1 = date(int(d[0]),int(d[1]),int(d[2]))
    diff = d1 - d0
    delta = datetime.timedelta(days=1)
    visit = visit_data[2].replace('[','').replace(']','')
    for x in visit.split(','):
      yield (  '_'.join( (str(d0), visit_data[3],visit_data[4])),int(x))
      d0+= delta


def get_table_format(data):
  for x in data:
    y = x[0].split('_')
    yield (y[0].split('-')[0],y[0].replace('2019','2020'),
           int(np.median(x[1])),
           list(count_low(x))[0],
           list(count_high(x))[0], 
           info_dic[(y[1],y[2])])

def count_low(x):
  low = round(int(np.median(x[1])) - int(np.std(x[1])))
  if low < 0:
    yield 0
  else:
    yield low

def count_high(x):
  high = round(int(np.median(x[1])) + int(np.std(x[1])))
  if high < 0:
    yield 0
  else:
    yield high

  
if __name__=='__main__':
  sc = pyspark.SparkContext()
  spark = SparkSession(sc)
  categories = [   
      ['452210','452311','big_box_grocers'],
      ['445120','convenience_stores'],
      ['722410','drinking_places'],
      ['722511','full_service_restaurants'],
      ['722513','limited_service_restaurants'],
      ['446110','446191','pharmacies_and_drug_stores'],
      ['311811','722515','snack_and_bakeries'],
      ['445210','445220','445230','445291','445292','445299','specialty_food_stores'],
      ['445110','supermarkets_except_convenience_stores'] ]

  id = ['452210','452311','445120','722410','722511','722513','446110','446191','311811','722515',
        '445210','445220','445230','445291','445292','445299','445110']

  path_name = ('test/big_box_grocers',
            'test/convenience_stores',
            'test/drinking_places',
            'test/full_service_restaurants',
            'test/limited_service_restaurants',
            'test/pharmacies_and_drug_stores',
            'test/snack_and_bakeries',
            'test/specialty_food_stores',
            'test/supermarkets_except_convenience_stores')
                  
  
    # hdfs:///data/share/bdm/core-places-nyc.csv
  get_info = sc.textFile('hdfs:///data/share/bdm/core-places-nyc.csv')\
              .map(lambda  x: get_name_id(next(csv.reader([x])))  )\
              .filter(lambda x: x is not None)\
              .cache()\
              .collect()
  data = list(itertools.chain(*get_info))
  info_dic = {}
  for y in get_info:
    y = ((y[0],y[1]),y[2])
    d = dict(itertools.zip_longest(*[iter(y)] * 2, fillvalue=""))
    key = list(d.keys())[0]
    info_dic[key] = d[key]
  
#  'hdfs:///data/share/bdm/weekly-patterns-nyc-2019-2020/*'
  rdd = sc.textFile('hdfs:///data/share/bdm/weekly-patterns-nyc-2019-2020/*')\
          .map(lambda x: get_pattern_info(next(csv.reader([x])) ))\
          .flatMap(visit_per_day)\
          .filter(lambda x: x[1] != 0 and x[0].split('_')[0] >'2018-12-31' and x[0].split('_')[0] < '2021-01-01')\
          .combineByKey(lambda x: [x], lambda x,y: x.append(y) , lambda x,y: x + y)\
          .sortBy(lambda x: x[0])\
          .mapPartitions(get_table_format)\
          .coalesce(1)\
          # .toDF(["year", "date" , "median","low","high","categorie"])\
          # .write.format("csv")\
          # .option("header", "true")\
          # .save(lambda x: 'test/'.join(x['categorie']))

          

          

      



  
             
  
    
