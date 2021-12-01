import sys
import csv
import datetime
import json
import pyspark
import numpy as np
from pyspark.sql import SparkSession
from pyspark import SparkContext
from datetime import date

def visit_per_day(visit_data):
  
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
  low = round(int(np.median(x[1])) - int(np.std(x[1])))
  if low < 0:
    return 0
  else:
    return low

def count_high(x):
  high = round(int(np.median(x[1])) + int(np.std(x[1])))
  if high < 0:
    return 0
  else:
    return high
  
if __name__=='__main__':
  sc = pyspark.SparkContext()
  spark = SparkSession(sc)
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
    # hdfs:///data/share/bdm/core-places-nyc.csv
    get_info = sc.textFile('hdfs:///data/share/bdm/core-places-nyc.csv')\
                .filter(lambda x: next(csv.reader([x]))[9] in categorie)\
                .coalesce(1)\
                .map(lambda x: next(csv.reader([x]))[0:2] )\
                .collect()
