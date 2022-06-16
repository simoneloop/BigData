import findspark
findspark.init()

from spark_core import *

from http.server import HTTPServer,BaseHTTPRequestHandler
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, DoubleType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import FloatType
from pyspark.sql.types import DoubleType

import json
import numpy as np
import os
import time


HOST='localhost'
PORT=8080


ALL_FUNC=['migliorRapportoCo2Kwh',
          'potenzaMediaKW','emissioniMediaCO2eqMinuto','potenzaMediaDisponibileNelloStatoKW',
          'potenzaMediaUtilizzataPerFonti','potenzaMediaInstallataPerFonti','emissioniMediaCO2eqMinutoPerFonti',
          'potenzaInEsportazioneMedia','potenzaInImportazioneMedia',
          'emissioniInEsportazioneMedia','emissioniInImportazioneMedia',
          'distribuzioneDellaEnergiaDisponibileNelTempo',
          'distribuzioneDellaEnergiaePotenzaDisponibileNelTempo',
          'distribuzioneDelleEmissioniNelTempo',
          'dbScan']

TEST_FUNC=['test','params','init']


def get_params(path) :
    path = path.replace("%", " ")
    if ('?' in path) :
        param = path.split('?')[1]

        params = param.split("&")
        res = {}
        for p in params :
            tmp = p.split("=")

            if ("[" in tmp[1] or "]" in tmp[1]) :
                list = tmp[1].strip('][').split(',')
                res[tmp[0]] = list
            else :
                res[tmp[0]] = tmp[1]
        return res
    else :
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
        special=False
        if(service_address in ALL_FUNC):
            self.send_response(200)
            self.send_header('content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin','*')
            self.end_headers()

            if (service_address == "migliorRapportoCo2Kwh"):
                rows = migliorRapportoCo2Kwh(df1,params)

            elif (service_address == 'potenzaMediaKW'):
                rows = potenzaMediaKW(df1, params)

            elif (service_address == "emissioniMediaCO2eqMinuto"):
                rows = emissioniMediaCO2eqMinuto(df1, params)

            elif (service_address == "potenzaInEsportazioneMedia") :
                rows = potenzaInEsportazioneMedia(df1, params)

            elif (service_address == "potenzaInImportazioneMedia") :
                rows = potenzaInImportazioneMedia(df1, params)

            elif (service_address == "potenzaMediaDisponibileNelloStatoKW") :
                rows = potenzaMediaDisponibileNelloStatoKW(df1, params)

            elif (service_address == "potenzaMediaDisponibileNelloStatoKW") :
                rows = potenzaMediaDisponibileNelloStatoKW(df1, params)

            elif (service_address == "emissioniInEsportazioneMedia") :
                rows = emissioniInEsportazioneMedia(df1, params)

            elif (service_address == "emissioniInImportazioneMedia") :

                rows = emissioniInImportazioneMedia(df1, params)
            elif (service_address == "potenzaMediaUtilizzataPerFonti"):
                rows = potenzaMediaUtilizzataPerFonti(df1, params)
                special = True
            elif (service_address == "potenzaMediaInstallataPerFonti") :
                rows = potenzaMediaInstallataPerFonti(df1, params)
                special = True
            elif (service_address == "emissioniMediaCO2eqMinutoPerFonti") :
                rows = emissioniMediaCO2eqMinutoPerFonti(df1, params)
                special = True
            elif (service_address == "distribuzioneDellaEnergiaDisponibileNelTempo") :
                rows = distribuzioneDellaEnergiaDisponibileNelTempo(df1, params)
                special = True
            elif (service_address == "distribuzioneDellaEnergiaePotenzaDisponibileNelTempo") :
                rows = distribuzioneDellaEnergiaePotenzaDisponibileNelTempo(df1, params)
                special = True
            elif (service_address == "distribuzioneDelleEmissioniNelTempo") :
                rows = distribuzioneDelleEmissioniNelTempo(df1, params)
                special = True

            elif (service_address == "dbScan"):
                rows = dbScan(df1, params)
                special = True

            if (rows == BAD_REQUEST) :
                files = bad_request
            else:
                if (special):
                    files=rows
                else:
                    files = [json.loads(row[0]) for row in rows]
            self.wfile.write(json.dumps(files).encode())

        elif(service_address in TEST_FUNC):
            self.send_response(200)
            self.send_header('content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()

            if service_address =='test':

                map={}
                map['hello']="world";
                response=json.dumps(map)
                self.wfile.write(response.encode())

            elif(service_address=="params"):
                response=json.dumps(params)
                self.wfile.write(response.encode())

            elif (service_address == "init"):
                self.wfile.write(json.dumps(INIT_MAP).encode())

        else:
            self.send_response(404)


#todo-*-*-*--*-*-*--*-*-*--*-*-*--*-*-*--*-*-*--*-*-*--*-*-*--*-*-*--*-*-*--server--*-*-*--*-*-*--server--*-*-*--*-*-*--*-*-*--*-*-*--*-*-*--*-*-*--*-*-*--*-*-*--*-*-*--*-*-*--*-*-*--*-*-*--*-*-*-
#todo-*-*-*--*-*-*--*-*-*--*-*-*--*-*-*--*-*-*--*-*-*--*-*-*--*-*-*--*-*-*--server--*-*-*--*-*-*--server--*-*-*--*-*-*--*-*-*--*-*-*--*-*-*--*-*-*--*-*-*--*-*-*--*-*-*--*-*-*--*-*-*--*-*-*--*-*-*-
def server():

    print("Run Spark")
    spark = SparkSession.builder.master("local[*]").appName('Core').getOrCreate()

    df = spark.read.csv(path + "/totalstates.csv", header=True, inferSchema=True)

    df = df.withColumn("total_production", repair_total_production(df['total_production'], df['exchange_import']))
    df = df.withColumn("total_emissions", repair_total_emissions(df['total_emissions'], df['exchange_import']))

    df = df.withColumn("stato_maggiore", stato_maggiore(df["stato"]))
    '''
    averaged = df.select('timestamp', 'stato_maggiore', 'carbon_intensity').groupBy('timestamp', 'stato_maggiore').avg()
    df = df.join(averaged,
                 (df['timestamp'] == averaged['timestamp']) & (df['stato_maggiore'] == averaged['stato_maggiore']),
                 "inner").drop(df.timestamp).drop(df.stato_maggiore)
    '''
    df = df.withColumn("fascia_oraria", fascia_oraria(df["timestamp"]))

    df = df.withColumn("consumo", map_consumo(df['total_production'], df['exchange_import'], df['exchange_export']))


    #todo Gestione potenza ed emissioni import
    df = df.withColumn("sum_import", sum_import_export(df['exchange_import']))

    df = df.withColumn("sum_import_stato_maggiore", sum_import_export_stato_maggiore(df['exchange_import'],df['stato_maggiore']))

    df = df.withColumn("sum_import_emissions", sum_import_export_emissions(df['exchange_import']))

    df = df.withColumn("sum_import_emissions_stato_maggiore", sum_import_export_emissions_stato_maggiore(df['exchange_import'],df['stato_maggiore']))
    #todo Gestione potenza ed emissioni import

    #todo *-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*

    #todo Gestione potenza ed emissioni export
    df = df.withColumn("sum_export", sum_import_export(df['exchange_export']))

    df = df.withColumn("sum_export_stato_maggiore", sum_import_export_stato_maggiore(df['exchange_export'],df['stato_maggiore']))

    df = df.withColumn("sum_export_emissions", sum_import_export_emissions(df['exchange_export']))

    df = df.withColumn("sum_export_emissions_stato_maggiore", sum_import_export_emissions_stato_maggiore(df['exchange_export'],df['stato_maggiore']))
    #todo Gestione potenza ed emissioni export

    #todo *-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*

    #todo Gestione timestamp in secondi
    df = df.select([unix_timestamp(("timestamp"), "HH:mm dd-MM-yyyy").alias("timestamp_inSeconds"), *col_union_new])
    #todo Gestione timestamp in secondi

    #todo *-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-cache()-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-cache()-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*
    global df1
    df1 = df.cache()
    global INIT_MAP
    INIT_MAP = init_map_server(df1)

    #todo *-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-cache()-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-cache()-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*

    server_address=(HOST,PORT)
    server=HTTPServer(server_address,SparkServer)
    print('Server running on port %s' % PORT)
    server.serve_forever()


if __name__=='__main__':
    server()

