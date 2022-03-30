import requests
import selenium
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager
import time
import os
import xlsxwriter
import pandas as pd
import re
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
url_elettricity_map="https://app.electricitymap.org/map"

if __name__ == '__main__':

    index_to_start=input("inserisci l'indice dal quale partire")
    index_to_stop=input("inserisci l'indice nel quale fermarti <=162")
    stati=['Austria', 'Belgio', 'Bulgaria', 'Cipro', 'Croazia', 'Danimarca', 'Estonia', 'Finlandia', 'Francia', 'Germania', 'Grecia', 'Irlanda', 'Italia', 'Lettonia', 'Lituania', 'Lussemburgo', 'Malta', 'Paesi Bassi', 'Polonia', 'Portogallo', 'Repubblica Ceca', 'Romania', 'Slovacchia', 'Slovenia', 'Spagna', 'Svezia', 'Ungheria']
    browser = webdriver.Chrome(r'C:\Users\simone\Desktop\chromedriver.exe');
    browser.get(url_elettricity_map);
    x_button=browser.find_elements(By.CLASS_NAME,"modal-close-button")[0]
    time.sleep(5)
    x_button.click()
    zone_list=browser.find_elements(By.CLASS_NAME,"zone-list")[0]
    #print("numero zone",len(zone_list))
    zones=zone_list.find_elements(By.TAG_NAME,"a")
    for i in range (int(index_to_start),int(index_to_stop)):
        zone_list = browser.find_elements(By.CLASS_NAME, "zone-list")[0]
        zones=zone_list.find_elements(By.TAG_NAME,"a")
        z=zones[i]
        tag=z.text.split("\n")
        stato=tag[len(tag)-1]
        if(stato in stati):
            z.click()
            time.sleep(1)
            left_panel=browser.find_elements(By.CLASS_NAME,"left-panel-zone-details")[0]
            time.sleep(1)
            rows = browser.find_elements(By.CLASS_NAME, "row")
            time.sleep(1)
            action = ActionChains(browser)
            for r in rows:
                action.move_to_element(r).perform()
                time.sleep(1.5)
                body=browser.find_elements(By.TAG_NAME,"body")[0]
                production_popup=None
                exchange_popup=None
                try:
                    production_popup=body.find_element_by_id("countrypanel-production-tooltip")
                except:
                    try:
                        exchange_popup=body.find_element_by_id("countrypanel-exchange-tooltip")
                    except:
                        print("no")
                if(production_popup):
                    print(production_popup.text)
                elif(exchange_popup):
                    print(exchange_popup.text)
                # popup=body.split("\ni\n")#sbagliato perché ci sono più i

                # print(popup)
                # if(len(popup)==2):
                #     popup=popup[1]
                #     print(popup)
                print("###########################################")

            back=browser.find_elements(By.CLASS_NAME,"left-panel-back-button")[0]
            back.click()
