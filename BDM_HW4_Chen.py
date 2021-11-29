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
  categories = [set(['452210','452311']),set(['445120']),set(['722410']),set(['722511']),
              set(['722513']), set(['446110','446191']),set(['311811','722515']),
                set( ['445210','445220','445230','445291','445292','445299']), 
                set(['445110'])]
  for index,categorie in enumerate(categories):
    get_info = sc.textFile('hdfs:///data/share/bdm/core-places-nyc.csv')\
                .filter(lambda x: next(csv.reader([x]))[9] in categorie)\
                .map(lambda x: next(csv.reader([x])))\
                .map(lambda x: (x[0],x[1]))\
                .collect()
            
    get_visit_data = sc.textFile('hdfs:///data/share/bdm/weekly-patterns-nyc-2019-2020/*')\
                   .filter(lambda x: tuple(next(csv.reader([x]))[0:2]) in  get_info)\
                   .map(lambda x: next(csv.reader([x])))\
                   .map(lambda x: (x[12].split('T')[0],x[13].split('T')[0],x[16]))\
                   .flatMap(visit_per_day)

    get_res_table = get_visit_data.filter(lambda x:x[1] != 0 and x[0] >'2018-12-31' and x[0] < '2021-01-01')\
                                             .groupByKey()\
                                             .mapValues(list)\
                                             .sortBy(lambda x: x[0])\
                                             .map(lambda x: (x[0].split('-')[0],x[0].replace('2019','2020'),int(np.median(x[1])),count_low(x),count_high(x)))
  
    df = get_res_table.map(lambda x: (x[0], x[1] , x[2],x[3],x[4]))\
                      .toDF(["year", "date" , "median","low","high"])
   
    
    df.repartition(1)\
      .write.format("csv")\
      .option("header", "true")\
      .save(path_name[index])              
          
             
  
    