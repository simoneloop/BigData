import findspark
findspark.init()
import time
from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import *
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, DoubleType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import FloatType
from pyspark.sql.types import DoubleType
from http.server import HTTPServer,BaseHTTPRequestHandler
import json
from spark_core import *


HOST='localhost'
PORT=8080
REQUEST_FILTER_ONE='func1'

ALL_FUNC=['func1','func2','func3','init','migliorRapportoCo2Kwh','potenzaMediaKW','emissioniMediaCO2eqMinuto','potenzaMediaUtilizzataPerFonti','potenzaMediaInstallataPerFonti','emissioniMediaCO2eqMinutoPerFonti','xyz']

def get_params(path):

    if('?' in path):
        param=path.split('?')[1]
       
        params=param.split("&")
        res={}
        for p in params:
            tmp=p.split("=")

            if("[" in tmp[1] or "]" in tmp[1]):
                list=tmp[1].strip('][').split(',')
                res[tmp[0]]=list
            else:
                res[tmp[0]]=tmp[1]
        return res
    else:
        return None

def get_service_address(path):
    serviceAddress=path
    if ('?' in path) :
        serviceAddress=path.split("?")[0]
        serviceAddress = serviceAddress.split("/")
        serviceAddress = serviceAddress[len(serviceAddress) - 2]

    serviceAddress = serviceAddress.split("/")
    serviceAddress = serviceAddress[len(serviceAddress) - 1]
    return serviceAddress

class SparkServer(BaseHTTPRequestHandler):
    def do_GET(self):
        params=get_params(self.path)
        service_address=get_service_address(self.path)
        print(params)
        print(service_address)
        if(service_address in ALL_FUNC):
            self.send_response(200)
            self.send_header('content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin','*')
            self.end_headers()

            if service_address =='func1':

                map={}
                map['hello']="world";
                response=json.dumps(map)
                self.wfile.write(response.encode())
            elif(service_address=="func2"):
                response=json.dumps(params)
                self.wfile.write(response.encode())
            elif(service_address=="func3"):
                rows=prova(df1)
                files = [json.loads(row[0]) for row in rows]
                self.wfile.write(json.dumps(files).encode())

            elif (service_address == "init"):
                map = {}
                tmp=[]
                stati = df1.select('stato_maggiore').distinct().collect()
                for s in stati:
                    tmp.append(s[0])
                map['stati'] = tmp

                tmp = []
                stati_sottostati = df1.select('stato').distinct().collect()
                for s in  stati_sottostati:
                    tmp.append(s[0])
                map['stati_sottostati'] = tmp

                inizio = df1.select(first('timestamp')).collect()
                fine = df1.select(last('timestamp')).collect()

                map['start_time'] = inizio[0]
                map['end_time'] = fine[0]
                map['fonti']=fonti
                self.wfile.write(json.dumps(map).encode())
            elif (service_address == "migliorRapportoCo2Kwh"):
                rows = migliorRapportoCo2Kwh(df1,params)
                files = [json.loads(row[0]) for row in rows]
                self.wfile.write(json.dumps(files).encode())
            elif (service_address == 'potenzaMediaKW'):
                rows = potenzaMediaKW(df1, params)
                files = [json.loads(row[0]) for row in rows]
                self.wfile.write(json.dumps(files).encode())
            elif (service_address == "emissioniMediaCO2eqMinuto"):
                rows = emissioniMediaCO2eqMinuto(df1, params)
                files = [json.loads(row[0]) for row in rows]
                self.wfile.write(json.dumps(files).encode())
            elif (service_address == "potenzaMediaUtilizzataPerFonti"):
                rows = potenzaMediaUtilizzataPerFonti(df1, params)
                files = [json.loads(row[0]) for row in rows]
                self.wfile.write(json.dumps(files).encode())
            elif (service_address == "potenzaMediaInstallataPerFonti") :
                rows = potenzaMediaInstallataPerFonti(df1, params)
                files = [json.loads(row[0]) for row in rows]
                self.wfile.write(json.dumps(files).encode())
            elif (service_address == "emissioniMediaCO2eqMinutoPerFonti") :
                rows = emissioniMediaCO2eqMinutoPerFonti(df1, params)
                files = [json.loads(row[0]) for row in rows]
                self.wfile.write(json.dumps(files).encode())
            elif (service_address == "xyz") :
                rows = xyz(df1, params)
                files = [json.loads(row[0]) for row in rows]
                self.wfile.write(json.dumps(files).encode())

        else:
            self.send_response(404)

def main():
    #---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    print("Run Spark")
    spark = SparkSession.builder.master("local[*]").appName('Core').getOrCreate()

    df = spark.read.csv(path + "/totalstates.csv", header=True, inferSchema=True)

    df = df.withColumn("stato_maggiore", stato_maggiore(df["stato"]))

    df = df.withColumn("total_production", repair_total_production(df['total_production'], df['exchange_import']))
    df = df.withColumn("total_emissions", repair_total_emissions(df['total_emissions'], df['exchange_import']))

    averaged = df.select('timestamp', 'stato_maggiore', 'carbon_intensity').groupBy('timestamp', 'stato_maggiore').avg()
    df = df.join(averaged,
                 (df['timestamp'] == averaged['timestamp']) & (df['stato_maggiore'] == averaged['stato_maggiore']),
                 "inner").drop(df.timestamp).drop(df.stato_maggiore)

    df = df.withColumn("fascia_oraria", fascia_oraria(df["timestamp"]))

    df = df.withColumn("consumo", map_consumo(df['total_production'], df['exchange_import'], df['exchange_export']))

    df = df.withColumn("sum_import", sum_import_export(df['exchange_import']))

    df = df.withColumn("sum_export", sum_import_export(df['exchange_export']))

    df = df.select([unix_timestamp(("timestamp"), "HH:mm dd-MM-yyyy").alias("timestamp_inSeconds"), *col_union])
    global df1
    df1 = df.cache()
    df1.count()

    #---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    server_address=(HOST,PORT)
    server=HTTPServer(server_address,SparkServer)
    print('Server running on port %s' % PORT)
    server.serve_forever()
    #---------------------------------------------------------------------------------------------------------------------------------------------------------------------------

if __name__=='__main__':
    main()

