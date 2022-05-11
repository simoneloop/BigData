from pyspark.sql import SparkSession

import pandas as pd
import os


if __name__ == '__main__':
    print("INIZIO")
    spark = SparkSession.builder.appName('core').getOrCreate()

    path = "./states"

    print(os.listdir(path))
    for f in os.listdir(path):
        print(f)
        #xcel = pd.read_excel(path + "/Austria.xlsx")

        #excel.to_csv("./statesCSV/Austria.csv", index=False)

        #rdd = spark.sparkContext.textFile(path + "/Austria.csv")