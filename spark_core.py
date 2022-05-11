import time
import findspark
findspark.init()
from pyspark.sql import SparkSession

import pandas as pd
import os


if __name__ == '__main__':
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    print("INIZIO")

    spark = SparkSession.builder.appName('core').getOrCreate()


    path = "./states"

    print(os.listdir(path))
    for f in os.listdir(path):
        print(f)
        xcel = pd.read_excel(path + "/" + f)
        f=f.split(".")[0]


        xcel.to_csv("./statesCSV/"+ f +".csv", index=False)
        #rdd = spark.sparkContext.textFile(path + "/Austria.csv")

    #rdd = spark.sparkContext.textFile("./statesCSV/" + "Austria.csv")
    #rdd=rdd.collect()
    #print(rdd)
    #time.sleep(10000)