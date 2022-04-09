import requests
import selenium
from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager
import time
import os
import xlsxwriter
import pandas as pd
import re
from selenium.webdriver.chrome.options import Options



######################## flusso dati

#tutti i watt sono in kw
#tutti i g sono in kg(tranne carbon_intensity che è gco2/kwh)





#per ogni paese un file excel/csv
#ogni thread avrà accesso ai vari fogli excel a cui ha accesso per gli stati da cercare
#il singolo thread, apre il foglio excel, ne legge il contenuto, avvia la ricerca per una nuova riga, concatena la nuova ricerca ai vecchi risultati infine sovrascrive il file excel del paese specifico








dataframe=pd.DataFrame(columns=['timestamp','carbon_intensity','low emissions','renewable_emissions','total_elettricity','total_emissions'
    ,'nucleare_installed_capacity','nucleare_emissions','geotermico_installed_capacity','geotermico_emissions'
    ,'biomasse_installed_capacity','biomasse_emissions','carbone_installed_capacity','carbone_emissions'
    ,'eolico_installed_capacity','eolico_emissions','fotovoltaico_installed_capacity','fotovoltaico_emissions'
    ,'idroelettrico_installed_capacity','idroelettrico_emissions','accumuloidro_installed_capacity','accumuloidro_emissions'
    ,'batteriaaccu_installed_capacity','batteriaaccu_emissions','gas_installed_capacity','gas_emissions'
    ,'petrolio_installed_capacity','petrolio_emissions','sconosciuto_installed_capacity','sconosciuto_emissions'
    ,'exchange_export','exchange_import'])


chrome_options = Options()
chrome_options.add_argument("--headless")

arts=['dal/dalla','il/la']

def get_production_data(data):
    print("Production \n",data)
    text=data.split('\n')#array 8 elementi
    print(text)
    type=None
    production=None
    emissions=None

    flag_deposit=1;
    if(re.search('accumulato', text[0])):
        flag_deposit=-1

    for art in arts:
        if (re.search(art, text[0])):
            tmp=text[0].split(art)
            type=tmp[1]

    if(type):
        type=type.split('.')
        type = type[0].replace(" ", "")


    total_eletricity=text[1].split("/ ")[1].replace(")","")

    tmp = re.search("[A-Z]+", total_eletricity)
    total_eletricity = re.search("[0-9]+['.'][0-9]*|[0-9]+", total_eletricity)

    if (total_eletricity):
        tmp = tmp.group(0)
        total_eletricity = float(total_eletricity.group(0))

        if(tmp == 'GW'):
            total_eletricity=total_eletricity * 1000000
        elif (tmp == 'MW'):
            total_eletricity = total_eletricity * 1000
        elif (tmp == 'W'):
            total_eletricity = total_eletricity / 1000

        percentage_on_total = re.search("[0-9]+['.'][0-9]*|[0-9]+", text[0])

        if(percentage_on_total):
            percentage_on_total = float(percentage_on_total.group(0))  # prendo il risultato trovato
            print("                                                            ", percentage_on_total," % di questa fonte sul totale statale")
            production = total_eletricity * (percentage_on_total/100) * flag_deposit
            production=round(production,2)



    installed_capacity = text[4].split("/ ")[1].replace(")", "")
    tmp = re.search("[A-Z]+", installed_capacity)
    installed_capacity = re.search("[0-9]+['.'][0-9]*|[0-9]+", installed_capacity)

    if(installed_capacity):
        tmp = tmp.group(0)
        installed_capacity = float(installed_capacity.group(0))

        if (tmp == 'GW'):
            installed_capacity = installed_capacity * 1000000
        elif (tmp == 'MW'):
            installed_capacity = installed_capacity * 1000

        elif (tmp == 'W'):
            installed_capacity = installed_capacity / 1000

    total_emissions = text[7].split("/ ")[1]
    total_emissions = total_emissions.split(" ")[0]

    tmp = re.search("[a-z]+", total_emissions)
    total_emissions = re.search("[0-9]+['.'][0-9]*|[0-9]+", total_emissions)

    if (total_emissions):
        tmp = tmp.group(0)
        total_emissions = float(total_emissions.group(0))
        if(tmp == 't'):
            total_emissions = total_emissions * 1000
        percentage_on_total = re.search("[0-9]+['.'][0-9]*|[0-9]+", text[6])
        if(percentage_on_total):
            percentage_on_total = float(percentage_on_total.group(0))
            print("                                                            ", percentage_on_total, " % di inquinamento di questa fonte sul totale statale")
            emissions= total_emissions * (percentage_on_total / 100)
            emissions=round(emissions,2)

    print("                                                             Capacita' installata Totale", total_eletricity," KW")

    print("                                                             Emissioni totali", total_emissions, " kg CO2 equivalente al minuto")

    print("                                                             Tipo di fonte", type)

    print("                                                             Capacità installata",installed_capacity, "KW")

    print("                                                             Produzione", production, " KW")

    print("                                                             Emissioni per questa fonte", emissions, " kg CO2 equivalente al minuto")
    return percentage_on_total,total_eletricity,total_emissions,type,production,emissions
    #TODO parser

def get_exchange_data(data):
    text = data.split('\n')
    res_import=[] #TODO
    res_export=[] #TODO
    exchange_state=None
    flag_deposit=None
    if (re.search('esportata', text[0])):
        exchange_state = text[0].split("in")[1]
        flag_deposit = -1
    else:
        flag_deposit = 1;
        exchange_state = text[0].split("da")[1]


    total_eletricity = text[1].split("/ ")[1].replace(")", "")
    tmp = re.search("[A-Z]+", total_eletricity)
    total_eletricity = re.search("[0-9]+['.'][0-9]*|[0-9]+", total_eletricity)

    if (total_eletricity):
        tmp = tmp.group(0)
        total_eletricity = float(total_eletricity.group(0))

        if (tmp == 'GW'):
            total_eletricity = total_eletricity * 1000000
        elif (tmp == 'MW'):
            total_eletricity = total_eletricity * 1000
        elif (tmp == 'W'):
            total_eletricity = total_eletricity / 1000

        percentage_on_total = re.search("[0-9]+['.'][0-9]*|[0-9]+", text[0])

        if (percentage_on_total):
            percentage_on_total = float(percentage_on_total.group(0))  # prendo il risultato trovato
            print("                                                            ", percentage_on_total,
                  " % di questa fonte sul totale statale")
            exchange = total_eletricity * (percentage_on_total / 100) * flag_deposit
            exchange = round(exchange, 2)

    print("stato scambio",exchange_state)

    installed_exchange_capacity = text[4].split("/ ")[1].replace(")", "")
    tmp = re.search("[A-Z]+", installed_exchange_capacity)
    installed_exchange_capacity = re.search("[0-9]+['.'][0-9]*|[0-9]+", installed_exchange_capacity)

    if (installed_exchange_capacity):
        tmp = tmp.group(0)
        installed_exchange_capacity = float(installed_exchange_capacity.group(0))

        if (tmp == 'GW'):
            installed_exchange_capacity = installed_exchange_capacity * 1000000
        elif (tmp == 'MW'):
            installed_exchange_capacity = installed_exchange_capacity * 1000

        elif (tmp == 'W'):
            installed_exchange_capacity = installed_exchange_capacity / 1000

    total_exchange_emissions = text[7].split("/ ")[1]
    total_exchange_emissions = total_exchange_emissions.split(" ")[0]

    tmp = re.search("[a-z]+", total_exchange_emissions)
    total_exchange_emissions = re.search("[0-9]+['.'][0-9]*|[0-9]+", total_exchange_emissions)

    if (total_exchange_emissions):
        tmp = tmp.group(0)
        total_exchange_emissions = float(total_exchange_emissions.group(0))
        if (tmp == 't'):
            total_exchange_emissions = total_exchange_emissions * 1000
        percentage_on_total = re.search("[0-9]+['.'][0-9]*|[0-9]+", text[6])
        if (percentage_on_total):
            percentage_on_total = float(percentage_on_total.group(0))
            print("                                                            ", percentage_on_total,
                  " % di inquinamento di questa fonte sul totale statale")
            emissions = total_exchange_emissions * (percentage_on_total / 100)
            emissions = round(emissions, 2)

    print("                                                             Capacita' installata Totale", total_eletricity,
          " KW")

    print("                                                             Emissioni  totali", total_exchange_emissions,
          " kg CO2 equivalente al minuto")

    print("                                                             Capacità installata", installed_exchange_capacity, "KW")

    print("                                                             Scambio", exchange, " KW")

    print("                                                             Emissioni per trasporto", emissions,
          " kg CO2 equivalente al minuto")

    return res_import,res_export
    #print("exchange \n", data)

    #TODO parser

def get_carbon_data(data):
    carbon_intensity=data[0].text.split("\n")[0].replace("g","")
    low_emissions=data[1].text.split("\n")[0]
    renewable_emissions=data[2].text.split("\n")[0]
    print(carbon_intensity,low_emissions,renewable_emissions)

    #TODO parser
# Press the green button in the gutter to run the script.

if __name__ == '__main__':
    url_elettricity_map = "https://app.electricitymap.org/map"

    stati=['Austria', 'Belgio', 'Bulgaria', 'Cipro', 'Croazia', 'Danimarca', 'Estonia', 'Finlandia', 'Francia',
              'Germania', 'Grecia', 'Irlanda', 'Italia', 'Lettonia', 'Lituania', 'Lussemburgo', 'Malta', 'Paesi Bassi',
              'Polonia', 'Portogallo', 'Repubblica Ceca', 'Romania', 'Slovacchia', 'Slovenia', 'Spagna', 'Svezia',
              'Ungheria']

    '''
    stati = ['Austria', 'Belgium', 'Bulgaria', 'Cyprus', 'Croatia', 'Denmark', 'Estonia', 'Finland', 'France',
            'Germany', 'Greece', 'Ireland', 'Italy', 'Latvia', 'Lithuania', 'Luxembourg', 'Malta', 'Netherlands',
             'Poland', 'Portugal', 'Czechia', 'Romania', 'Slovakia', 'Slovenia', 'Spain', 'Sweden',
             'Hungary']
    '''
    #stati = ['Francia']
    service = Service(executable_path=ChromeDriverManager().install())

    #s=Service("chromedriver.exe")
    browser = webdriver.Chrome(service=service)
    #browser = webdriver.Chrome(service=service,options=chrome_options)

    browser.get(url_elettricity_map)
    time.sleep(2)
    x_button=browser.find_elements(By.CLASS_NAME,"modal-close-button")[0]
    time.sleep(2)
    x_button.click()

    zone_list=browser.find_elements(By.CLASS_NAME,"zone-list")[0]
    zones=zone_list.find_elements(By.TAG_NAME,"a")

    print('Zone Totali Disponibili :                    ',len(zones))
    print('Stati per i quali vogliamo prendere i dati  :',len(stati))
    for i in range (0,len(zones)):
         zona = zones[i]
         tag = zona.text.split("\n")  # 2 elementi [ numero , nome_stato ] o 3 elementi [ numero ,zona_specifica, nome_stato ]
         stato = tag[len(tag) - 1]    # prendiamo nome_stato


         if (stato in stati):
            if (len(tag) == 3):
                zona_nello_stato = tag[len(tag) - 2]
                print("Stato = ", stato, "zona nello stato = ", zona_nello_stato)
            else:
                print("Stato = ", stato)

            zona.click()
            time.sleep(2)
            left_panel = browser.find_elements(By.CLASS_NAME, "left-panel-zone-details")[0] # cliccato il paese prendiamo il pannello a sinistra
            time.sleep(2)
            body=browser.find_elements(By.TAG_NAME,"body")[0]
            time.sleep(2)
            try:
                carbon_data=body.find_elements(By.CLASS_NAME,"country-col")
            except:
                pass
            if(carbon_data):
                get_carbon_data(carbon_data)

            rows = browser.find_elements(By.CLASS_NAME, "row") #prendiamo tutte le righe ognuna delle quali è una fonte energetica o scambio
            time.sleep(2)
            action = ActionChains(browser)
            for r in rows:
                action.move_to_element(r).perform()
                time.sleep(1.5)
                body = browser.find_elements(By.TAG_NAME, "body")[0]
                production_popup = None
                exchange_popup = None
                try:
                    production_popup = body.find_element(By.ID,"countrypanel-production-tooltip")
                except:
                    try:
                        exchange_popup = body.find_element(By.ID,"countrypanel-exchange-tooltip")
                    except:
                        pass
                if (production_popup):
                    print()
                    get_production_data(production_popup.text)

                elif (exchange_popup):
                    print()
                    res_import,res_export=get_exchange_data(exchange_popup.text)
                print("#############################################################################################################################################")
            back = browser.find_elements(By.CLASS_NAME, "left-panel-back-button")[0]
            back.click()
            zone_list = browser.find_elements(By.CLASS_NAME, "zone-list")[0]
            zones = zone_list.find_elements(By.TAG_NAME, "a")


    browser.close()

