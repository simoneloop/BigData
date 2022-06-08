import findspark
findspark.init()
import time
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

stato_maggiore = udf(lambda x: get_stato_maggiore(x), StringType())
fascia_oraria = udf(lambda x: get_fascia_oraria(x), StringType())
map_consumo = udf(lambda x, y, z: get_consumo(x, y, z), FloatType())
sum_import_export=udf(lambda x: get_sum_import_export(x), FloatType())
repair_total_production=udf(lambda x, y: get_new_total_production(x, y), FloatType())


col_static = ['timestamp_inMillis', 'timestamp' , 'carbon_intensity' , 'low_emissions' , 'renewable_emissions',
              'total_production', 'total_emissions', 'exchange_export', 'exchange_import', 'stato', 'consumo',
              'fascia_oraria']

col_classic = ['timestamp','fascia_oraria','stato_maggiore','stato','carbon_intensity','avg(carbon_intensity)','low_emissions','renewable_emissions',
               'total_production','total_emissions','consumo','nucleare_installed_capacity','nucleare_production','nucleare_emissions',
               'geotermico_installed_capacity','geotermico_production','geotermico_emissions','biomassa_installed_capacity','biomassa_production',
               'biomassa_emissions','carbone_installed_capacity','carbone_production','carbone_emissions','eolico_installed_capacity','eolico_production',
               'eolico_emissions','fotovoltaico_installed_capacity','fotovoltaico_production','fotovoltaico_emissions','idroelettrico_installed_capacity',
               'idroelettrico_production','idroelettrico_emissions','accumuloidro_installed_capacity','accumuloidro_production','accumuloidro_emissions',
               'batterieaccu_installed_capacity','batterieaccu_production','batterieaccu_emissions','gas_installed_capacity','gas_production',
               'gas_emissions','petrolio_installed_capacity','petrolio_production','petrolio_emissions','sconosciuto_installed_capacity',
               'sconosciuto_production','sconosciuto_emissions','exchange_export','sum_export','exchange_import','sum_import']

col_pro =       ['sum(total_production)','sum(total_emissions)','sum(nucleare_installed_capacity)',
                 'sum(nucleare_production)','sum(nucleare_emissions)','sum(geotermico_installed_capacity)','sum(geotermico_production)',
                 'sum(geotermico_emissions)','sum(biomassa_installed_capacity)','sum(biomassa_production)','sum(biomassa_emissions)',
                 'sum(carbone_installed_capacity)','sum(carbone_production)','sum(carbone_emissions)','sum(eolico_installed_capacity)',
                 'sum(eolico_production)','sum(eolico_emissions)','sum(fotovoltaico_installed_capacity)','sum(fotovoltaico_production)',
                 'sum(fotovoltaico_emissions)','sum(idroelettrico_installed_capacity)','sum(idroelettrico_production)','sum(idroelettrico_emissions)',
                 'sum(accumuloidro_installed_capacity)','sum(accumuloidro_production)','sum(accumuloidro_emissions)','sum(batterieaccu_installed_capacity)',
                 'sum(batterieaccu_production)','sum(batterieaccu_emissions)','sum(gas_installed_capacity)','sum(gas_production)','sum(gas_emissions)',
                 'sum(petrolio_installed_capacity)','sum(petrolio_production)','sum(petrolio_emissions)','sum(sconosciuto_installed_capacity)',
                 'sum(sconosciuto_production)','sum(sconosciuto_emissions)','sum(consumo)','sum(sum_import)','sum(sum_export)']

col_union=[]
for i in col_classic:
    col_union.append(i)
    for j in col_pro:
        if(j.find(i) != -1):
            #col_union.append(j)
            break


def get_sum_import_export(x):
    sum= 0
    try :
        n = x.split("@")
        for i in n:
            if (i):
                try :
                    value = i.split("_")[2]
                    if (value == "nan"):
                        value = 0
                    sum += float(value)
                except Exception as e:
                    print(e)
                    sum += 0
    except Exception as e:
        #print(e)
        sum += 0
    return sum


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
                    value = i.split("_")[2]
                    if (value == "nan"):
                        value = 0
                    import_q+= float(value)
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
                    value=i.split("_")[2]
                    if(value=="nan"):
                        value=0
                    export_q += float(value)
                except Exception as e:
                    print(e)
                    export_q += 0
    except Exception as e:
        #print(e)
        export_q += 0

    cont = float(x) + import_q + export_q
    return cont
def get_new_total_production(x,y):
    import_q = 0

    try:
        n = y.split("@")
        for i in n:
            if (i):
                try:
                    value=i.split("_")[2]
                    if(value=="nan"):
                        value=0
                    import_q += float(value)
                except Exception as e:
                    print(e)
                    import_q += 0
    except Exception as e:
        # print(e)
        import_q += 0
    return float(x)-import_q


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
            tmp=tmp.union(df.filter(df['timestamp_inSeconds']>=i).filter(df['timestamp_inSeconds']<i+millis_day))
        else:
            tmp=df.filter(df['timestamp_inSeconds']>=i).filter(df['timestamp_inSeconds']<i+millis_day)
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

    df = df.withColumn("stato_maggiore", stato_maggiore(df["stato"]))
    df = df.withColumn("total_production", repair_total_production(df['total_production'], df['exchange_import']))

    averaged = df.select('timestamp', 'stato_maggiore', 'carbon_intensity').groupBy('timestamp', 'stato_maggiore').avg()
    df = df.join(averaged,
                  (df['timestamp'] == averaged['timestamp']) & (df['stato_maggiore'] == averaged['stato_maggiore']),
                  "inner").drop(df.timestamp).drop(df.stato_maggiore)

    df = df.withColumn("fascia_oraria", fascia_oraria(df["timestamp"]))

    df = df.withColumn("consumo", map_consumo(df['total_production'], df['exchange_import'], df['exchange_export']))

    df = df.withColumn("sum_import", sum_import_export(df['exchange_import']))

    df = df.withColumn("sum_export", sum_import_export(df['exchange_export']))
    #df.filter(df['timestamp'] == "19:00 20-04-2022").filter(df['stato_maggiore'] == "Italia").show()


    df = df.select([unix_timestamp(("timestamp"), "HH:mm dd-MM-yyyy").alias("timestamp_inSeconds"),*col_union])

    print("siamo qua 1")
    start = time.time()
    df1 = df.cache()
    df1.count()
    print("Tempo di cache = ",time.time() - start)

    start = time.time()
    sum1= df1.select('stato','stato_maggiore','total_production').groupBy('stato','stato_maggiore').avg().groupBy('stato_maggiore').sum().sort(col('sum(avg(total_production))').desc())
    sum1.show()

    sum1 = df1.select('stato','total_production').groupBy('stato').avg().sort(col('avg(total_production)').desc())
    sum1.show()

    #query sul carbon_intensity stato intero
    sum1 = df1.select('stato_maggiore', 'carbon_intensity').groupBy('stato_maggiore').avg().sort(col('avg(carbon_intensity)').desc())
    sum1.show()
    # query sul carbon_intensity stato/sottostati
    sum1 = df1.select('stato', 'carbon_intensity').groupBy('stato').avg().sort(col('avg(carbon_intensity)').desc())
    sum1.show()


    sum1 = df1.select('timestamp', 'fotovoltaico_production').groupBy('timestamp').sum()
    sum1.show()
    print("Tempo = ", time.time() - start)



    '''
    df1.show()
    print(df1.count())

    print("siamo qua 2")
    df1.filter(df1['timestamp'] == "19:00 20-04-2022").filter(df1['stato_maggiore'] == "Italia").select("sum(sum_import)").show()
    df1.filter(df1['timestamp'] == "19:00 20-04-2022").filter(df1['stato_maggiore'] == "Italia").select("sum(sum_export)").show()
    print("siamo qua 3")
    #print(df1.describe())
    #df1.show()
    #df.show(300)

    #print(df.count())

    print(df1.filter(df1['carbon_intensity']>300).count())
    print(df1.filter(df1['carbon_intensity']<200).count())

    x=[1650492000,1650578400]
    df1=query_timestamp(df,x)

    y=['mattina','pomeriggio','sera','notte']
    df1=query_fascia_oraria(df1,y)


    stati=["Austria","Francia","Danimarca orientale (Danimarca)"]

    df1=query_stati(df1,stati).select('stato').distinct().show()
    fonti=['nucleare','geotermico']

    time.sleep(10000)
    df1=query_fonte(df,fonti)
    df1.show(300)

    df1.filter(df['stato_maggiore']=='Italia').dropDuplicates((['stato'])).show(300)

    #print("...",df.filter(df['timestamp_inMillis'] >= x).filter(df['timestamp_inMillis'] <= y).count())
    #df.filter(df['timestamp_inMillis'] >= x).filter(df['timestamp_inMillis'] <= y).show()
    #df.show(300)

    #time.sleep(10000)
    print("FINE")
    '''




