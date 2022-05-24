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
map_consumo = udf(lambda x: get_consumo(x), StringType())

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




def get_consumo(x):

    import_q = 0
    export_q = 0
    print
    for i in x[0].exchange_import.split("@"):
        if (i):
            import_q += float(i.split("_")[2])
    for i in x[0].exchange_export.split("@"):
        if (i):
            export_q += float(i.split("_")[2])
    
    cont = float(x[0].total_production) + import_q + export_q
    #print(cont)
    return cont


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


def query_timestamp(df, x, y):
    return df.filter(df['timestamp_inMillis']>=x).filter(df['timestamp_inMillis']<=y)

def query_fascia_oraria(df, M, P,S,N):

    return df.filter(df['fascia_orari']=='mattina')




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


    df = df.withColumn("consumo", map_consumo(df.select(struct('total_production', 'exchange_export', 'exchange_import').alias("structed"))))
    #struct('total_production', 'exchange_export', 'exchange_import'))


    #df = df.withColumn("fascia_oraria", fascia_oraria(df["timestamp"]))

    df = df.select([unix_timestamp(("timestamp"), "HH:mm dd-MM-yyyy").alias("timestamp_inMillis"),'*'])
    df1 = df.cache()
    print(df1.describe())
    df1.show()
    #df.show(300)

    #print(df.count())

    #print(df.filter(df['carbon_intensity']>300).count())
    #print(df.filter(df['carbon_intensity']<200).count())

    x=1650474000
    y=1650475200

    df=query_timestamp(df,x,y)
    df.show()

    #print("...",df.filter(df['timestamp_inMillis'] >= x).filter(df['timestamp_inMillis'] <= y).count())
    #df.filter(df['timestamp_inMillis'] >= x).filter(df['timestamp_inMillis'] <= y).show()
    #df.show(300)

    time.sleep(10000)