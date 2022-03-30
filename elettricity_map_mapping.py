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


chrome_options = Options()
chrome_options.add_argument("--headless")

def get_production_data(data):
    print("")
    # print(data)
    #TODO parser

def get_exchange_data(data):
    print("")
    # print(data)
    #TODO parser

def get_carbon_data(data):
    for d in data:
        print(d.text)
    #TODO parser
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    url_elettricity_map = "https://app.electricitymap.org/map"

    statiold=['Austria', 'Belgio', 'Bulgaria', 'Cipro', 'Croazia', 'Danimarca', 'Estonia', 'Finlandia', 'Francia',
              'Germania', 'Grecia', 'Irlanda', 'Italia', 'Lettonia', 'Lituania', 'Lussemburgo', 'Malta', 'Paesi Bassi',
              'Polonia', 'Portogallo', 'Repubblica Ceca', 'Romania', 'Slovacchia', 'Slovenia', 'Spagna', 'Svezia',
              'Ungheria']

    stati = ['Austria', 'Belgium', 'Bulgaria', 'Cyprus', 'Croatia', 'Denmark', 'Estonia', 'Finland', 'France',
            'Germany', 'Greece', 'Ireland', 'Italy', 'Latvia', 'Lithuania', 'Luxembourg', 'Malta', 'Netherlands',
             'Poland', 'Portugal', 'Czechia', 'Romania', 'Slovakia', 'Slovenia', 'Spain', 'Sweden',
             'Hungary']

    # stati = ['France']

    s=Service("chromedriver.exe")
    browser = webdriver.Chrome(service=s)
    # browser = webdriver.Chrome(service=s,options=chrome_options)

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
         tag = zona.text.split("\n")  # 2 elementi [ numero , nome_stato ]
         stato = tag[len(tag) - 1]    # prendiamo nome_stato

         if (stato in stati):
            zona.click()
            time.sleep(2)
            left_panel = browser.find_elements(By.CLASS_NAME, "left-panel-zone-details")[0] # cliccato il paese prendiamo il pannello a sinistra
            time.sleep(2)
            body=browser.find_elements(By.TAG_NAME,"body")[0]
            time.sleep(2)
            carbon_data=body.find_elements(By.CLASS_NAME,"country-col")
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
                    production_popup = body.find_element_by_id("countrypanel-production-tooltip")
                except:
                    try:
                        exchange_popup = body.find_element_by_id("countrypanel-exchange-tooltip")
                    except:
                        print("no")
                if (production_popup):
                    get_production_data(production_popup.text)

                elif (exchange_popup):
                    get_exchange_data(exchange_popup.text)
                print("###########################################")
            back = browser.find_elements(By.CLASS_NAME, "left-panel-back-button")[0]
            back.click()
            zone_list = browser.find_elements(By.CLASS_NAME, "zone-list")[0]
            zones = zone_list.find_elements(By.TAG_NAME, "a")



browser.close()

