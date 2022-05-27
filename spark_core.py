import findspark
findspark.init()
import time
import pandas as pd
from pyspark.sql import SparkSession
import os
import multiprocessing
from pyspark.sql.functions import *
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, DoubleType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import FloatType
from pyspark.sql.types import DoubleType

n_core = multiprocessing.cpu_count()
path = "./statesCSV"

precedent_dates_filters=None
new_date_filter=None


fascia_oraria = udf(lambda x: get_fascia_oraria(x), StringType())
map_consumo = udf(lambda x , y ,z: get_consumo(x,y,z), StringType())
stato_maggiore = udf(lambda x: get_stato_maggiore(x), StringType())


col_static = ['timestamp_inMillis', 'timestamp' , 'carbon_intensity' , 'low_emissions' , 'renewable_emissions',
              'total_production', 'total_emissions', 'exchange_export', 'exchange_import', 'stato', 'consumo',
              'fascia_oraria']





def get_stato_maggiore(x):
    try:
        return x.split("(")[1].replace(")", "")
    except:
        return x


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




def get_consumo(x,y,z):

    import_q = 0
    export_q = 0
    try:
        n=y.split("@")
        for i in n:
            if (i):
                try:
                    import_q=float(i.split("_")[2])
                except Exception as e:
                    print(e)
                    import_q += 0
    except Exception as e:
        #print(e)
        import_q += 0

    try :
        n = z.split("@")
        for i in n:
            if (i):
                try :
                    export_q = float(i.split("_")[2])
                except Exception as e:
                    print(e)
                    export_q += 0
    except Exception as e:
        #print(e)
        export_q += 0

    cont = float(x) + import_q + export_q
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
millis_day=86400
fasce_MPSN=['mattina','pomeriggio','sera','notte']

def query_timestamp(df, giorni):
    tmp=None
    for i in giorni:
        if(tmp):
            tmp=tmp.union(df.filter(df['timestamp_inMillis']>=i).filter(df['timestamp_inMillis']<i+millis_day))
        else:
            tmp=df.filter(df['timestamp_inMillis']>=i).filter(df['timestamp_inMillis']<i+millis_day)
    return tmp

def query_fascia_oraria(df, fasce):
    fasce_tmp = []
    for s in fasce:
        fasce_tmp.append('fascia_oraria="' + s + '"')

    return df.filter(" or ".join(fasce_tmp))

def query_stati(df, stati):
    stati_tmp=[]
    for s in stati:
        stati_tmp.append('stato="'+s+'"')

    return df.filter(" or ".join(stati_tmp))


def query_fonte(df, fonti):
    col_selezionate = col_static
    for f in fonti :
        col_selezionate.append(f + "_installed_capacity")
        col_selezionate.append(f + "_production")
        col_selezionate.append(f + "_emissions")

    return df.select(*col_selezionate)


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
    path1="./statesCSV/"
    print(os.listdir(path1))
    for f in os.listdir(path1):
        if(count == 0):
            df=pd.read_csv(path1 + f)
            count = 1
        else:
            df=pd.concat([df,pd.read_csv(path1 + f)])
    df.to_csv(path1 + "totalStates" + ".csv", index=False)
    '''
    df = spark.read.csv(path + "/totalstates.csv", header=True, inferSchema=True)

    #df=df.filter(df['stato']=='Austria')

    df = df.withColumn("stato_maggiore", stato_maggiore(df["stato"]))

    #df = df.withColumn("carbon_intensity_avg", carbon_intensity_avg(df['timestamp'],df["carbon_intensity"],df['stato_maggiore']))

    #sottoStati= df.filter(df['stato'])
    df.cache()

    tmp = df.filter('stato_maggiore != stato')
    timestamp_unique = tmp.dropDuplicates(['timestamp']).select('timestamp')

    #print(timestamp_unique.rdd.collect())
    dftmp=[]
    states_unique =tmp.dropDuplicates(['stato']).select('stato_maggiore').dropDuplicates(['stato_maggiore']).rdd.collect()
    for t in timestamp_unique.rdd.collect():
            for s in states_unique:
                #print(t[0],s[0])
                valori=tmp.filter(tmp['timestamp'] == t[0]).filter(tmp['stato_maggiore'] == s[0]).select('carbon_intensity')
                c= valori.count()
                somma= valori
                print(t[0],s[0],c)

    #tmp= tmp.filter()

    #time.show()
    #tmp.show(300)
    #df.filter(df['stato_maggiore'] == 'Italia').dropDuplicates((['stato'])).show(300)

    time.sleep(10000)
    df = df.withColumn("consumo", map_consumo(df['total_production'],df['exchange_import'],df['exchange_export']))

    df = df.withColumn("fascia_oraria", fascia_oraria(df["timestamp"]))


    df = df.select([unix_timestamp(("timestamp"), "HH:mm dd-MM-yyyy").alias("timestamp_inMillis"),'*'])
    df1 = df.cache()
    #print(df1.describe())
    #df1.show()
    #df.show(300)

    #print(df.count())

    #print(df.filter(df['carbon_intensity']>300).count())
    #print(df.filter(df['carbon_intensity']<200).count())

    x=[1650492000,1650578400]
    #df1=query_timestamp(df,x)

    y=['mattina','pomeriggio','sera','notte']
    #df1=query_fascia_oraria(df1,y)


    stati=["Austria","Francia","Danimarca orientale (Danimarca)"]

    #df1=query_stati(df1,stati)
    #.select('stato').distinct().show()
    fonti=['nucleare','geotermico']


    #df1=query_fonte(df,fonti)
    #df1.show(300)
    df.filter(df['stato_maggiore']=='Italia').dropDuplicates((['stato'])).show(300)

    #print("...",df.filter(df['timestamp_inMillis'] >= x).filter(df['timestamp_inMillis'] <= y).count())
    #df.filter(df['timestamp_inMillis'] >= x).filter(df['timestamp_inMillis'] <= y).show()
    #df.show(300)

    #time.sleep(10000)





