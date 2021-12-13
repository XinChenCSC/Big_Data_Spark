import csv
import datetime
import json
import pyspark
import numpy as np
from pyspark.sql import SparkSession
from pyspark import SparkContext
from datetime import date

import itertools
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType, MapType, StringType

def get_name(x):
    for data in categories:
      if x in data:
        return data[-1]

def visit_per_day(date_start,date_end,visits_by_day):
  
  d = date_start.split('T')[0].split('-')
  d0 = date(int(d[0]),int(d[1]),int(d[2]))
  d = date_end.split('T')[0].split('-')
  d1 = date(int(d[0]),int(d[1]),int(d[2]))
  diff = d1 - d0
  delta = datetime.timedelta(days=1)
  visit = visits_by_day.replace('[','').replace(']','')
  res = {}
  for x in visit.split(','):
    x_int = int(x)
    d0_str = str(d0)
    if x_int != 0 and d0_str >'2018-12-31' and d0_str  < '2021-01-01':
      res [d0_str] = x_int
    d0+= delta
  return res




def count_low(x):
  
  low = round(int(np.median(x)) - int(np.std(x)))
  if low < 0:
    return 0
  else:
    return low

def count_high(x):
  
  high = round(int(np.median(x)) + int(np.std(x)))
  if high < 0:
    return 0
  else:
    return high


  
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

  path_name = {
            'big_box_grocers':'test/big_box_grocers',
            'convenience_stores':'test/convenience_stores',
            'drinking_places':'test/drinking_places',
            'full_service_restaurants':'test/full_service_restaurants',
            'limited_service_restaurants':'test/limited_service_restaurants',
            'pharmacies_and_drug_stores':'test/pharmacies_and_drug_stores',
            'snack_and_bakeries': 'test/snack_and_bakeries',
            'specialty_food_stores':'test/specialty_food_stores',
            'supermarkets_except_convenience_stores':'test/supermarkets_except_convenience_stores'}
              
  
    # hdfs:///data/share/bdm/core-places-nyc.csv
    
  udf_get_name = F.udf(get_name, StringType())

  id_info = spark.read.csv('hdfs:///data/share/bdm/core-places-nyc.csv', header=True, escape='"') \
                      .where(F.col('naics_code').isin(id))\
                      .withColumn('name', udf_get_name('naics_code') )\
                      .select('safegraph_place_id','name')
  
  
  
  udf_visit_per_day = F.udf(visit_per_day, MapType(StringType(), IntegerType()))
  udf_count_median = F.udf(lambda x: int(np.median(x)) ,IntegerType())
  udf_count_low = F.udf(count_low,IntegerType())
  udf_count_high = F.udf(count_high,IntegerType())
  udf_get_year = F.udf(lambda x: x.replace('2019','2020'), StringType())
  visit_info = spark.read.csv('hdfs:///data/share/bdm/weekly-patterns-nyc-2019-2020/*', header=True, escape='"')\

  
                    

  res = visit_info.join(id_info, on = "safegraph_place_id", how = 'left'  ) \
                  .dropna()\
                  .select('name',
                          F.explode(udf_visit_per_day('date_range_start','date_range_end','visits_by_day'))\
                          .alias('date','visits'))\
                  .groupBy('name','date')\
                  .agg(F.collect_list('visits').alias('visits'))\
                  .sort('date')\
                  .withColumn('median',udf_count_median('visits') )\
                  .withColumn('low',udf_count_low('visits') )\
                  .withColumn('high',udf_count_high('visits') )\
                  .withColumn("year",F.split(F.col('date'),'-')[0]   )\
                  .withColumn("date",udf_get_year(F.col('date')) )



  for key,value in path_name.items():
    res.where(F.col('name') == key)\
        .select('year','date','median','low','high')\
        .write.csv(value.replace('test',sys.argv[1]) if len(sys.argv)>1 else value)



