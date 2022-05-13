import time
import findspark
findspark.init()
from pyspark.sql import SparkSession

import pandas as pd
import os


if __name__ == '__main__':

    from pyspark.sql import SparkSession

    print("INIZIO")

    spark = SparkSession.builder.appName('core').getOrCreate()
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
    df.to_csv("./statesCSV/" + "totalstates" + ".csv", index=False)
    '''
    rdd0 = spark.sparkContext.textFile("C:/workSpacepy/BigData/statesCSV/totalstates.csv")

    #for r in rdd3.collect():
    #    print(r)
    #rdd0.max(carbon"_intensity)

    #rdd = spark.sparkContext.textFile(path + "/Austria.csv")

    #rdd = spark.sparkContext.textFile("./statesCSV/" + "Austria.csv")
    #rdd=rdd.collect()
    #print(rdd)
    #time.sleep(10000)