
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
import datetime
import json
import numpy as np
import sys

def expandVisits(date_range_start, visits_by_day):
    expand_visit_lst= []
    d_start = datetime.datetime(*map(int,date_range_start[:10].split('-')))

    for days,visits in enumerate(json.loads(visits_by_day) ):
        if visits == 0: continue
        d = d_start + datetime.timedelta(days=days)
        if d.year in (2019,2020):
          expand_visit_lst.append((d.year, f'{d.month:02d}-{d.day:02d}', visits))
    return expand_visit_lst


def computeStats(group, visits):

    visits = np.fromiter(visits,np.int_)
    visits.resize(groupCount[group])
    median = np.median(visits)
    std = np.std(visits)
    return (int(median+0.5), max(0,int(median - std +0.5)), int(median + std +0.5)  )


if __name__=='__main__':
    sc = SparkContext()
    spark = SparkSession(sc)

    dfPlaces = spark.read.csv('hdfs:///data/share/bdm/core-places-nyc.csv', header=True, escape='"')
    dfPattern = spark.read.csv('hdfs:///data/share/bdm/weekly-patterns-nyc-2019-2020/*', header=True, escape='"')
    OUTPUT_PREFIX = sys.argv[1]
    categories = {
              'big_box_grocers': {'452210','452311'} ,
              'convenience_stores':{'445120'},
              'drinking_places':{'722410'},
              'full_service_restaurants':{'722511'},
              'limited_service_restaurants':{'722513'},
              'pharmacies_and_drug_stores':{'446110','446191'},
              'snack_and_bakeries':  {'311811','722515'},
              'specialty_food_stores':{'445210','445220','445230','445291','445292','445299'},
              'supermarkets_except_convenience_stores':{'445110'}}

    CAT_CODES = {x for x_set in categories.values() for x in x_set }
    CAT_GROUP = {x:index for index,x_set in enumerate(categories.values()) for x in x_set }
    dfD = dfPlaces.  \
              filter(F.col('naics_code').isin(CAT_CODES))\
              .select('placekey','naics_code')

    udfToGroup = F.udf(CAT_GROUP.get, T.IntegerType())

    dfE = dfD.withColumn('group', udfToGroup('naics_code'))
    dfF = dfE.drop('naics_code').cache()
    groupCount= dict(dfF.groupBy(dfF.group).count().collect())

    visitType = T.StructType([T.StructField('year', T.IntegerType()),
                              T.StructField('date', T.StringType()),
                              T.StructField('visits', T.IntegerType())])

    udfExpand = F.udf(expandVisits, T.ArrayType(visitType))

    dfH = dfPattern.join(dfF, 'placekey') \
        .withColumn('expanded', F.explode(udfExpand('date_range_start', 'visits_by_day'))) \
        .select('group', 'expanded.*')



    statsType = T.StructType([T.StructField('median', T.IntegerType()),
                              T.StructField('low', T.IntegerType()),
                              T.StructField('high', T.IntegerType())])

    udfComputeStats = F.udf(computeStats, statsType)

    dfI = dfH.groupBy('group', 'year', 'date') \
        .agg(F.collect_list('visits').alias('visits')) \
        .withColumn('stats', udfComputeStats('group', 'visits'))
    dfJ = dfI \
        .select('group','year','date','stats.*')\
        .orderBy('group','year','date')\
        .withColumn('date',F.concat(F.lit('2020-'), F.col('date') ))\
        .cache()


    categories = list(categories.keys())


    for index,filename in enumerate(categories):
      dfJ.filter(f'group={index}') \
          .drop('group') \
          .coalesce(1) \
          .write.csv(f'{OUTPUT_PREFIX}/{filename}' if OUTPUT_PREFIX != None else f'test/{filename}',
                    mode='overwrite', header=True)




