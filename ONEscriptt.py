import requests
import json
import html
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime
from selenium import webdriver
import time
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import urllib3
import os
#C4

file_path=r'C:\Users\bot\Barakat Vegetables & Fruits Co. (L.L.C.)\Data Analytics - Documents\Dynamics Datasets\Ecom Dataset\WebScrap'

HEAD = {'User_Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0'}
try: 
#LULU
    print('LULU Web scrapping started with Chrome Driver')
    service = Service(executable_path='C:/Users/bot/Downloads/chromedriver-win64/chromedriver.exe')
    options = webdriver.ChromeOptions()
    driver = webdriver.Chrome(service=service, options=options)
    scroll_pause_time = 1
    driver.get("https://www.luluhypermarket.com/en-ae/grocery-fresh-food-fruits-vegetables/c/HY00216090")
    SCROLL_PAUSE_TIME = 10
    last_height = driver.execute_script("return document.body.scrollHeight")
    while True:
       
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_PAUSE_TIME)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
     
    href_links = []  
    product_links = driver.find_elements(By.CLASS_NAME, 'js-gtm-product-link')
     
    for link in product_links:
         href = link.get_attribute("href")  
         href_links.append(href)  
     
     
    HEAD = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 OPR/105.0.0.0'}
     
    import pandas as pd
     
     
     
    def get_name(soup):  
        try:
            return soup.find("h1", class_='product-name').text.strip()
        except AttributeError:
            return ""
     
    def get_og_price(soup):  
        try:
            price_element = soup.find('span', class_="off" ).text  
            import re
            match = re.search(r'\d+\.\d+', price_element)  
            if match:
             extracted_price = match.group()
            return extracted_price
        except AttributeError:
            return ""
     
    def get_company():
        try:
            return "LULU"
        except AttributeError:
            return ""
     
    def get_1():
        try:
            return None
        except AttributeError:
            return ""
     
    def get_date():
            return datetime.now().date()
     
     
    def get_current_price(soup):  
        try:
            price_element = soup.find('span', class_="current").find('span', class_='item price').text
            import re
            match = re.search(r'\d+\.\d+', price_element)  
            if match:
             extracted_price = match.group()  
            return extracted_price
        except AttributeError:
            return ""
     
    lulu = {"COMPANY_NAME": [],"DATE": [],"PRODUCT_ID": [],"PRODUCT_NAME": [],"U_O_M": [],"PRICE": [],"DISCOUNTED_PRICE": [],"COUNTRY_OF_ORIGIN": []}
     
    for link in href_links:
        new_response= requests.get(link, headers=HEAD)
        final_soup = BeautifulSoup(new_response.content, "html.parser")
        lulu['PRODUCT_NAME'].append(get_name(final_soup))  
        lulu['COMPANY_NAME'].append(get_company())
        lulu['PRICE'].append(get_og_price(final_soup))  
        lulu['DISCOUNTED_PRICE'].append(get_current_price(final_soup))
        lulu['DATE'].append(get_date())
        lulu['PRODUCT_ID'].append(get_1())
        lulu['U_O_M'].append(get_1())
        lulu['COUNTRY_OF_ORIGIN'].append(get_1())
     
     
    dflulu = pd.DataFrame(lulu)
    dflulu['PRICE'] = dflulu['PRICE'].replace('', np.NAN)
    dflulu['PRICE'] = dflulu['PRICE'].fillna(dflulu['DISCOUNTED_PRICE'])
    file_name_lulu='competitordata_lulu.csv'
    input_file_path=os.path.join(file_path,file_name_lulu)
    dflulu.to_csv(input_file_path, index=False)
    print(dflulu)
    print('LULU Web scrapping completed with Chrome Driver')
    print(datetime.now())

    
except urllib3.exceptions.HTTPError as e:
    print("HTTPError:", e)
##except urllib3.exceptions.URLError as e:
##    print("URLError:", e)
except Exception as e:
    print("Error:", e)

#vegetablesoukfruits
try:
    
    print('VegeatableSouk fruits started')
    
    page_numbers= range(1,5)
    all_external_urlsF = []
    url = "https://cst0dljetj.execute-api.ap-south-1.amazonaws.com/Production/content/zWOrH-c5P942toMgFbh/listproducts"


    for page_number in page_numbers:
        querystring = {"id":"60b5e1fb88261c0012c69539","perPage":"48","page":str(page_number)}


        payload = ""
        headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "en-US",
            "authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiI2NWIzNmRiMjliZTNkYzAwMTk3N2ZkMDkiLCJuYW1lIjoiZ3Vlc3QtbXdaQ3p0ekF5IiwiZW1haWwiOiJndWVzdC1td1pDenR6QXkiLCJpYXQiOjE3MDYyNTc4NDJ9.MyxizHNCVcHEM-9_YxMWBsp9xI-KC_HSl3__To3J0FY",
            "origin": "https://www.vegetablesouk.com",
            "priority": "u=1, i",
            "referer": "https://www.vegetablesouk.com/",
            "^sec-ch-ua": "^\^Chromium^^;v=^\^124^^, ^\^Microsoft",
            "sec-ch-ua-mobile": "?0",
            "^sec-ch-ua-platform": "^\^Windows^^^",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "cross-site",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
            "x-api-key": "Dnx6IkTNgO8vOQrgKwyza6RxfLfJA8Mu3RetJcGy"
        }

        response = requests.request("GET", url, data=payload, headers=headers, params=querystring)

        dataVF = response.json()
        

        for item in dataVF["data"]["hits"]:
            
            external_url = item["externalUrl"]

            
            all_external_urlsF.append(external_url)
     
    def get_title(soup):
     try:
        title=soup.find('div', class_='pdp-heart').text.strip()
        return title
     except AttributeError:
        return ""

    def get_price(soup):
        try:
            price = soup.find('span', class_='t3-mainPrice me-3').text.strip()
            import re
            match = re.search(r'\d+(?:\.\d+)?', price)  
            if match:
             pricevegf = match.group()  
            return pricevegf
        except AttributeError:
            return ""

    def get_country(soup):
        try:
         country=soup.find('div', class_='country').text.strip()
         return country
        except AttributeError:
            return ""

    def get_quantity(soup):
        try:
            quant = soup.find('div', class_='approx-div').find('p').text.strip()
            if "Notes" in quant:
                quant_clean=quant.split("Notes")[1].strip()
                return quant_clean
            else:
                return quant
        except AttributeError:
            return ""
        
    def get_date():
            return datetime.now().date()
        

    def get_company1():
        try:
            return "VegetableSouk"
        except AttributeError:
            return ""
    def get_1():
        try:
            return None
        except AttributeError:
            return ""
        
    vegsoukF = {"COMPANY_NAME": [],
            "DATE": [],
            "PRODUCT_ID": [],
            "PRODUCT_NAME": [],
            "U_O_M": [],
            "PRICE": [],
            "DISCOUNTED_PRICE": [],
            "COUNTRY_OF_ORIGIN": []}

    for new_1 in all_external_urlsF:

            response2 = requests.get(new_1, headers=HEAD)
            new_soup = BeautifulSoup(response2.text, "html.parser")
            vegsoukF['PRODUCT_NAME'].append(get_title(new_soup))
            vegsoukF['PRICE'].append(get_price(new_soup))
            vegsoukF['COUNTRY_OF_ORIGIN'].append(get_country(new_soup))
            vegsoukF['COMPANY_NAME'].append(get_company1())
            vegsoukF['DATE'].append(get_date())
            vegsoukF['PRODUCT_ID'].append(get_1())
            vegsoukF['U_O_M'].append(get_quantity(new_soup))
            vegsoukF['DISCOUNTED_PRICE'].append(get_1())
            
        

    dfvegf = pd.DataFrame(vegsoukF)
    dfvegf['PRODUCT_NAME'] = dfvegf['PRODUCT_NAME'] + ' ' + dfvegf['U_O_M']
    print('VegeatableSouk Fruits completed')
    file_name_soukveg='competitordata_soukfruits.csv'
    input_file_path=os.path.join(file_path,file_name_soukveg)
    dfvegf.to_csv(input_file_path, index=False)

    print(dfvegf)
    print(datetime.now())
    print('VegeatableSouk Fruits Completed')
    
except urllib3.exceptions.HTTPError as e:
    print("HTTPError:", e)
##except urllib3.exceptions.URLError as e:
##    print("URLError:", e)
except Exception as e:
    print("Error:", e)


#vegsouk vegeatables
try:
    
    print('VegeatableSouk Veg Started')
    page_numbers= range(1,5)
    url = "https://cst0dljetj.execute-api.ap-south-1.amazonaws.com/Production/content/zWOrH-c5P942toMgFbh/listproducts"

    all_external_urlsV = []
    for page_number in page_numbers:
        querystring = {"id":"60c4a9c1752cd20012f36b93","perPage":"48","page":str(page_number)}

        payload = ""
        headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "en-US",
            "authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiI2NWIzNmRiMjliZTNkYzAwMTk3N2ZkMDkiLCJuYW1lIjoiZ3Vlc3QtbXdaQ3p0ekF5IiwiZW1haWwiOiJndWVzdC1td1pDenR6QXkiLCJpYXQiOjE3MDYyNTc4NDJ9.MyxizHNCVcHEM-9_YxMWBsp9xI-KC_HSl3__To3J0FY",
            "origin": "https://www.vegetablesouk.com",
            "priority": "u=1, i",
            "referer": "https://www.vegetablesouk.com/",
            "^sec-ch-ua": "^\^Chromium^^;v=^\^124^^, ^\^Microsoft",
            "sec-ch-ua-mobile": "?0",
            "^sec-ch-ua-platform": "^\^Windows^^^",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "cross-site",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
            "x-api-key": "Dnx6IkTNgO8vOQrgKwyza6RxfLfJA8Mu3RetJcGy"
        }

        response = requests.request("GET", url, data=payload, headers=headers, params=querystring)

        #print(response.text)

        dataVV = response.json()

        for item in dataVV["data"]["hits"]:
            
            external_url = item["externalUrl"]

            
            all_external_urlsV.append(external_url)

    HEAD = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0'}
    def get_title(soup):

     try:
        title=soup.find('div', class_='pdp-heart').text.strip()
        return title
     except AttributeError:
            return ""
    
    def get_price(soup):
        try:
            price = soup.find('span', class_='t3-mainPrice me-3').text.strip()
            import re
            match = re.search(r'\d+(?:\.\d+)?', price)  
            if match:
             price_vegv = match.group()  
            return price_vegv
        except AttributeError:
            return ""

    def get_country(soup):
        try:
         country=soup.find('div', class_='country').text.strip()
         return country
        except AttributeError:
            return ""

    def get_company1():
        try:
            return "VegetableSouk"
        except AttributeError:
            return ""

    def get_quantity(soup):
        try:
            quant = soup.find('div', class_='approx-div').find('p').text.strip()
            if "Notes" in quant:
                quant_clean=quant.split("Notes")[1].strip()
                return quant_clean
            else:
                return quant
        except AttributeError:
            return ""

    def get_date():
            return datetime.now().date()

    def get_1():
        try:
            return None
        except AttributeError:
            return ""
        
    soukV = {"COMPANY_NAME": [],
            "DATE": [],
            "PRODUCT_ID": [],
            "PRODUCT_NAME": [],
            "U_O_M": [],
            "PRICE": [],
            "DISCOUNTED_PRICE": [],
            "COUNTRY_OF_ORIGIN": []}
    for new in all_external_urlsV:
            response1 = requests.get(new, headers=HEAD)
            new_soup_1 = BeautifulSoup(response1.text, "html.parser")
            soukV['PRODUCT_NAME'].append(get_title(new_soup_1))
            soukV['PRICE'].append(get_price(new_soup_1))
            soukV['COUNTRY_OF_ORIGIN'].append(get_country(new_soup_1))
            soukV['COMPANY_NAME'].append(get_company1())
            soukV['DATE'].append(get_date())
            soukV['PRODUCT_ID'].append(get_1())
            soukV['U_O_M'].append(get_quantity(new_soup_1))
            soukV['DISCOUNTED_PRICE'].append(get_1())
            

    dfvegv = pd.DataFrame(soukV)
    dfvegv['PRODUCT_NAME'] = dfvegv['PRODUCT_NAME'] + ' ' + dfvegv['U_O_M']
    file_name_soukveg='competitordata_soukveg.csv'
    input_file_path=os.path.join(file_path,file_name_soukveg)
    dfvegv.to_csv(input_file_path, index=False)

  
    print(datetime.now())
    print('VegeatableSouk Veg Completed')

    
except urllib3.exceptions.HTTPError as e:
    print("HTTPError:", e)
##except urllib3.exceptions.URLError as e:
##    print("URLError:", e)
except Exception as e:
    print("Error:", e)


#C4
try:
    print('C4 Webscrap started')
    url = 'https://www.carrefouruae.com/api/v8/categories/F11600000?sortBy=relevance&currentPage=0&pageSize=2000&areaCode=Dubai%20Festival%20City%20-%20Dubai&lang=en&expressPos=015&displayCurr=AED&foodPos=072&nonFoodPos=099&latitude=25.217392942107853&longitude=55.36187758635983&requireSponsProducts=true'
     
    payload = {}
     
    headers = {
     
    }
     
    response = requests.request("GET", url, headers=headers, data=payload)
     
    dataC = response.json()
     
    product_data = []
     
    for items in dataC["products"]:
     
        tp = {"COMPANY_NAME": "Carrefour",
            "DATE": datetime.now().date(),
            "PRODUCT_ID": [],
            "PRODUCT_NAME": [],
            "U_O_M": [],
            "PRICE": [],
            "DISCOUNTED_PRICE": [],
            "COUNTRY_OF_ORIGIN": []}
     
        tp["PRODUCT_ID"] = items["id"]
     
        tp["PRODUCT_NAME"] = html.unescape(items["name"])
       
     
        if "productOrigin" in items:
     
            tp["COUNTRY_OF_ORIGIN"] = items["productOrigin"]
     
       
        if not tp["COUNTRY_OF_ORIGIN"]:
                 tp["COUNTRY_OF_ORIGIN"] = None
     
       
        if "price" in items:
     
            tp["PRICE"] = items["price"]["formattedValue"]
     
            if "discount" in items["price"]:
     
                tp["DISCOUNTED_PRICE"] = items["price"]["discount"]["formattedValue"]
     
            if not tp["DISCOUNTED_PRICE"]:
                 tp["DISCOUNTED_PRICE"] = None
     
        if "size" in items:
     
            tp["U_O_M"] = items["size"]
       
        if not tp["U_O_M"]:
                 tp["U_O_M"] = None
     
       
     
        product_data.append(tp)
     
    dfc4 = pd.DataFrame(product_data)

     
     
    dfc4["PRICE"]=dfc4["PRICE"].str.replace("AED","")
    dfc4["DISCOUNTED_PRICE"]=dfc4["DISCOUNTED_PRICE"].str.replace("AED","")

    
    file_name_c4='competitordata_c4.csv'
    input_file_path=os.path.join(file_path,file_name_c4)
    dfc4.to_csv(input_file_path, index=False) 
    print(dfc4)

    #dfc4
    print('C4 Webscrap completed')
except urllib3.exceptions.HTTPError as e:
    print("HTTPError:", e)
##except urllib3.exceptions.URLError as e:
##    print("URLError:", e)
except Exception as e:
    print("Error:", e)

 
#KIBSON
try:
    print('KIBSON Fruits Webscrap started')
    url = "https://www.kibsons.com/ecomapi/itemdetails/getItemsV2"     
    payload = '{"category":["F"],"":[""],"language":"en","searchText":""}'
     
    headers = {
     
        
     
    }
     
    response = requests.request("POST", url, headers=headers, data=payload)
     
    dataKF = response.json()
     
    product_data1 = []
     
    for items in dataKF["data"]:
     
        tp = {"COMPANY_NAME": "KIBSON","DATE": datetime.now().date(),"PRODUCT_ID": [],"PRODUCT_NAME": [],"U_O_M": [],"PRICE": [],"DISCOUNTED_PRICE": [],"COUNTRY_OF_ORIGIN": []}
     
        tp["PRODUCT_ID"] = items["serialNo"]
     
        tp["PRODUCT_NAME"] = items["stockDesc"]
     
        tp["PRICE"] = items["stockRate"]
     
        if "stockPromotionRate" in items:
     
            tp["DISCOUNTED_PRICE"] = items["stockPromotionRate"]
     
        #tp["ribbonText"] = items["ribbonText"]
     
        tp["U_O_M"] = items["stockShortDetail"]
     
        tp["COUNTRY_OF_ORIGIN"] = items["stockOrigin"]
     
        product_data1.append(tp)
     
    dfk1 = pd.DataFrame(product_data1)
    dfk1['PRODUCT_NAME'] = dfk1['PRODUCT_NAME'] + ' ' + dfk1['U_O_M']
    file_name_kbf='competitordata_kibsonfruits.csv'
    input_file_path=os.path.join(file_path,file_name_kbf)
    dfk1.to_csv(input_file_path, index=False) 
    print(dfk1)

    print('KIBSON Fruits Webscrap completed')
    
except urllib3.exceptions.HTTPError as e:
    print("HTTPError:", e)
##except urllib3.exceptions.URLError as e:
##    print("URLError:", e)
except Exception as e:
    print("Error:", e)
 
 
try:
    print('KIBSON Veg Webscrap started')    
    url = "https://www.kibsons.com/ecomapi/itemdetails/getItemsV2"
     
    payload = '{"category":["V"],"":[""],"language":"en","searchText":""}'
     
    headers = {
     
        
     
    }
     
    response = requests.request("POST", url, headers=headers, data=payload)
     
    dataKV = response.json()
     
    product_data = []
     
    for items in dataKV["data"]:
     
        tp = {"COMPANY_NAME": "KIBSON",
            "DATE": datetime.now().date(),
            "PRODUCT_ID": [],
            "PRODUCT_NAME": [],
            "U_O_M": [],
            "PRICE": [],
            "DISCOUNTED_PRICE": [],
            "COUNTRY_OF_ORIGIN": []}
     
        tp["PRODUCT_ID"] = items["serialNo"]
     
        tp["PRODUCT_NAME"] = items["stockDesc"]
     
        tp["PRICE"] = items["stockRate"]
     
        if "stockPromotionRate" in items:
     
            tp["DISCOUNTED_PRICE"] = items["stockPromotionRate"]
     
        #tp["ribbonText"] = items["ribbonText"]
     
        tp["U_O_M"] = items["stockShortDetail"]
     
        tp["COUNTRY_OF_ORIGIN"] = items["stockOrigin"]
     
        #tp["stockUnits"] = items["stockUnits"]
     
        #tp["category"] = items["family_en_desc"]
     
        product_data.append(tp)
     
    dfk2 = pd.DataFrame(product_data)
    dfk2['PRODUCT_NAME'] = dfk2['PRODUCT_NAME'] + ' ' + dfk2['U_O_M']
    #dfk = pd.concat([dfk1, dfk2], ignore_index=True)
    print(dfk2)
    
    file_name_kbv='competitordata_kibsonveg.csv'
    input_file_path=os.path.join(file_path,file_name_kbv)
    dfk2.to_csv(input_file_path, index=False) 

    
    print('KIBSON Veg Webscrap completed')
    
except urllib3.exceptions.HTTPError as e:
    print("HTTPError:", e)
##except urllib3.exceptions.URLError as e:
##    print("URLError:", e)
except Exception as e:
    print("Error:", e)
  


    
#df4 = pd.concat([dfvegf, dfvegv], ignore_index=True)
#df12 = pd.concat([dfc4, dfk], ignore_index=True)
#df34 = pd.concat([dflulu, df4], ignore_index=True)
#dffinal = pd.concat([df12, df34], ignore_index=True)
#dffinal
#insert your file_path
#file_path = r'C:\Users\bot\Barakat Vegetables & Fruits Co. (L.L.C.)\Data Analytics - Documents\Dynamics Datasets\Ecom Dataset\WebScrap\competitordata.csv'
 
#dffinal.to_csv(file_path, index=False)


