import findspark
findspark.init()
import time
from pyspark.sql import SparkSession
import os
import multiprocessing
from pyspark.sql.functions import *
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import FloatType

n_core = multiprocessing.cpu_count()
path = "./statesCSV"

precedent_dates_filters=None
new_date_filter=None


fascia_oraria = udf(lambda x: get_fascia_oraria(x), StringType())
map_consumo = udf(lambda x, y, z: get_consumo(x, y, z), StringType())

def get_fascia_oraria(x):
    hh = x.split(":")[0]
    hh = int(hh)
    if (hh >= 00 and hh < 6):
        return "notte"
    elif (hh >= 6 and hh < 12):
        return "mattina"
    elif (hh >= 12 and hh < 18):
        return "pomeriggio"
    elif (hh >= 18 and hh <= 23):
        return "sera"




def get_consumo(x, y, z):

    import_q = 0
    export_q = 0

    for i in y.split("@"):
        if (i):
            import_q += float(i.split("_")[2])
    for i in z.split("@"):
        if (i):
            export_q += float(i.split("_")[2])

    cont = x + import_q + export_q
    return format(float(cont), 'f')


#giorni>fasciaoraria>stati/sottostati>fonti
# if(filtrifasciaoraria==full):
#     fasciaoraria=giorni
# else:
#     fasciaoraria=filt(filtrifasciaoraria(giorni))
#


# if(precedent_dates_filters!=new_date_filter):
#     pass
#     #recalculate date database
#
# def filter_on_dates(dates):
#     pass
#     #return new database
# def get_nstates_on_source(n,source_list):
#     pass
#






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
    df = spark.read.csv(path + "/totalstates.csv", header=True, inferSchema=True)


    df = df.withColumn("consumo", map_consumo(df['total_production'], df['exchange_import'], df['exchange_export']))

    df = df.withColumn("fascia_oraria", fascia_oraria(df["timestamp"]))

    df = df.select([unix_timestamp(("timestamp"), "HH:mm dd-MM-yyyy").alias("timestamp_inMillis"),'*'])

    #df.show(300)
    print("att")
    time.sleep(10)
    print("attfine")
    print(df.count())

    print(df.filter(df['carbon_intensity']>300).count())
    print(df.filter(df['carbon_intensity']<200).count())
    #df.show(300)
