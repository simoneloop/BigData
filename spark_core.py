import findspark
findspark.init()
import time
from pyspark.sql import SparkSession
import pandas as pd
import os
import multiprocessing
from enum import Enum
class col(Enum):
    timestamp: 0
    carbon_intensity: 1
    low_emissions: 2

#n_core = multiprocessing.cpu_count()


if __name__ == '__main__':
    print("INIZIO")

    spark = SparkSession.builder.master("local[*]").appName('Core').getOrCreate()

    #print(spark.getActiveSession())


    '''
    path = "./states"

    print(os.listdir(path))

    for f in os.listdir(path):
        print(f)
        xcel = pd.read_excel(path + "/" + f)
        f=f.split(".")[0]
        xcel["stato"]=f

        xcel.to_csv("./statesCSV/"+ f +".csv", index=False)
   
    df=0
    count=0
    path1="./statesCSV"
    print(os.listdir(path1))
    for f in os.listdir(path1):
        if(count == 0):
            df=pd.read_csv(path1 + "/" + f)
            count = 1
        else:
            df=pd.concat([df,pd.read_csv(path1 + "/" + f)])
    df.to_csv(path1 + "totalStates" + ".csv", index=False)
    '''

    sc=spark.sparkContext
    rdd0 = sc.parallelize(sc.textFile("C:/workSpacepy/BigData/statesCSV/totalStates.csv").collect(),numSlices=1000)
    print("initial partition count:" + str(rdd0.getNumPartitions()))


    rdd1 = rdd0.map(lambda x:(x,1))
    print(rdd1.collect())
    #for r in rdd1.collect():
        #print(r[col.carbon_intensity])

    #print(rdd0.take(4))
    #rdd1 = rdd0.map(lambda x: )
    #df2 = df.select([unix_timestamp(("timestamp"), "HH:mm dd-MM-yyyy").alias("timestamp_inMillis"), ('carbon_intensity')])
    #df2.show()

    #for r in rdd3.collect():
    #    print(r)
    #rdd0.max(carbon"_intensity)

    #rdd = spark.sparkContext.textFile(path + "/Austria.csv")

    #rdd = spark.sparkContext.textFile("./statesCSV/" + "Austria.csv")
    #rdd=rdd.collect()
    #print(rdd)
    #time.sleep(10000)