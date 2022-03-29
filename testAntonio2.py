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
# This is a sample Python script.

# Press Maiusc+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    url_elettricity_map = "https://app.electricitymap.org/map"

    statiold=['Austria', 'Belgio', 'Bulgaria', 'Cipro', 'Croazia', 'Danimarca', 'Estonia', 'Finlandia', 'Francia',
              'Germania', 'Grecia', 'Irlanda', 'Italia', 'Lettonia', 'Lituania', 'Lussemburgo', 'Malta', 'Paesi Bassi',
              'Polonia', 'Portogallo', 'Repubblica Ceca', 'Romania', 'Slovacchia', 'Slovenia', 'Spagna', 'Svezia',
              'Ungheria']

    #stati = ['Austria', 'Belgium', 'Bulgaria', 'Cyprus', 'Croatia', 'Denmark', 'Estonia', 'Finland', 'France',
   #          'Germany', 'Greece', 'Ireland', 'Italy', 'Latvia', 'Lithuania', 'Luxembourg', 'Malta', 'Netherlands',
    #          'Poland', 'Portugal', 'Czechia', 'Romania', 'Slovakia', 'Slovenia', 'Spain', 'Sweden',
    #          'Hungary']

    stati = ['France']

    s=Service("C:\\workSpacepy\\bigdataprove\\chromedriver.exe")
    browser = webdriver.Chrome(service=s)

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
            rows = browser.find_elements(By.CLASS_NAME, "row") #prendiamo tutte le righe ognuna delle quali è una fonte energetica o scambio
            time.sleep(2)
            action = ActionChains(browser)
            for r in rows:
                action.move_to_element(r).perform()
                time.sleep(2)
                try:
                    info = browser.find_element(By.ID, "countrypanel-production-tooltip")
                    print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
                    if(len(info.text) != 0):
                             info = browser.find_element(By.ID, "countrypanel-production-tooltip")
                             print(info.text)
                             print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
                    else:
                             info1 = browser.find_elements(By.ID, "countrypanel-exchange-tooltip")
                             print(info1.text)

                except:
                        print("LINEA VUOTA")


            back = browser.find_elements(By.CLASS_NAME, "left-panel-back-button")[0]
            back.click()
            zone_list = browser.find_elements(By.CLASS_NAME, "zone-list")[0]
            zones = zone_list.find_elements(By.TAG_NAME, "a")



browser.close()

