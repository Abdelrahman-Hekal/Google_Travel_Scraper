from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ActionChains
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService 
from subprocess import CREATE_NO_WINDOW 
from datetime import datetime
import time
import warnings
import os
import re
import sys
import unidecode
import pandas as pd
import pandas as pd
# google API dependencies
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.http import MediaFileUpload
# UI dependencies
import tkinter
from tkinter import ttk
from ttkthemes import ThemedTk
import threading
import asyncio
from tkinter import messagebox
import tkinter.scrolledtext as ScrolledText
import logging
import pickle
import re
import numpy as np
from currency_symbols import CurrencySymbols
import pycountry

warnings.filterwarnings('ignore')
NoneType = type(None)
 

def initialize_bot(driver_path):

    # Setting up chrome driver for the bot
    chrome_options  = webdriver.ChromeOptions()
    # suppressing output messages from the driver
    chrome_options.add_argument('--log-level=3')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-extensions')
    chrome_options.add_argument('--window-size=1920,1080')
    # adding user agents
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36")
    chrome_options.add_argument("--incognito")
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
    # running the driver with no browser window
    chrome_options.add_argument('--headless=new')
    chrome_options.page_load_strategy = 'normal'
    #if not images:
    #    prefs = {"profile.managed_default_content_settings.images": 2}
    #    chrome_options.add_experimental_option("prefs", prefs)
    # installing the chrome driver
    #driver_path = ChromeDriverManager().install()
    chrome_service = ChromeService(driver_path)
    chrome_service.creationflags = CREATE_NO_WINDOW
    chrome_service.creation_flags = CREATE_NO_WINDOW
    # configuring the driver
    driver = webdriver.Chrome(driver_path, options=chrome_options, service=chrome_service)
    driver.set_page_load_timeout(30)
    driver.maximize_window()
    return driver

def get_Google_API_creds():
    
    global running, root, dummy_driver

    dummy_driver = get_url('https://www.google.com', dummy_driver)

    SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/documents', 'https://www.googleapis.com/auth/drive']

    # API configuration
    credentials = None
    if os.path.exists('token.json'):
        try:
            credentials = Credentials.from_authorized_user_file('token.json', SCOPES)
        except Exception as err:
            credentials = None
    try:
        # If there are no (valid) credentials available, let the user log in.
        connected = False
        if not credentials or not credentials.valid:
            try:
                if credentials and credentials.expired and credentials.refresh_token:
                    dummy_driver = get_url('https://www.google.com', dummy_driver)
                    credentials.refresh(Request())
                    connected = True
            except Exception as err:
                pass
            if not connected:
                dummy_driver = get_url('https://www.google.com', dummy_driver)
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                ports = [8080, 7100, 6100, 5100, 4100]               
                for port in ports:
                    try:
                        dummy_driver = get_url('https://www.google.com', dummy_driver)
                        credentials = flow.run_local_server(port=port, access_type='offline', include_granted_scopes='true')
                        connected = True
                        break
                    except Exception as err:
                        output_msg('The below error occurred while authenicating the Google API ....', 1)
                        output_msg(str(err), 0)
                if not connected:
                    output_msg('Failed to authenicate the Google API, could not find an empty port, exiting ....', 1)
                    sys.exit()
                # Save the credentials for the next run
                with open('token.json', 'w') as token:
                    token.write(credentials.to_json())

    except Exception as err:
        output_msg('The below error occurred while authenicating the Google API ....', 1)
        output_msg(str(err), 0)
        sys.exit()

    time.sleep(2)

    return credentials

def process_sheet(url):

    global dummy_driver
    dummy_driver = get_url('https://www.google.com', dummy_driver)

    credentials = get_Google_API_creds()
    try:
        sheets_service = build('sheets', 'v4', credentials=credentials, cache_discovery=False, static_discovery=False)
    except Exception as err:
      DISCOVERY_SERVICE_URL = 'https://sheets.googleapis.com/'
      sheets_service = build('sheets', 'v4', credentials=credentials, discoveryServiceUrl=DISCOVERY_SERVICE_URL, cache_discovery=False, static_discovery=False)

    # getting all the destinations infor from the template sheet
    dummy_driver = get_url('https://www.google.com', dummy_driver)
    sheet_row_count = get_sheet_row_count(sheets_service, url)
    dummy_driver = get_url('https://www.google.com', dummy_driver)
    rows = read_range(sheets_service, url, sheet_row_count)

    # trimming the list by the first empty row in the sheet
    dests = []
    for row in rows:
        try:
            if row[0] != '' and row[0] != 'nan' and row[0] != None:
                if len(row) == 5:
                    dests.append((row[0], row[1], row[2], row[3], row[4]))
                elif len(row) == 4:
                    dests.append((row[0], row[1], row[2], row[3]))
                elif len(row) == 3:
                    dests.append((row[0], row[1], row[2]))
                elif len(row) == 2:
                    dests.append((row[0], row[1], ''))
                else:
                    dests.append((row[0], '', ''))
            else:
                break
        except Exception as err:
            break

    return dests

def get_sheet_row_count(sheets_service, url):

    global dummy_driver
    dummy_driver = get_url('https://www.google.com', dummy_driver)

    spreadsheet_id = url.split('/')[5]
    sheet_metadata = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    dummy_driver = get_url('https://www.google.com', dummy_driver)
    sheets = sheet_metadata.get('sheets', '')
    for sheet in sheets:
        name_sheet = sheet["properties"]["title"]
        if name_sheet == "Sheet1":
            return sheet["properties"]["gridProperties"]["rowCount"]

def read_range(sheets_service, url, row_count_max):

    global dummy_driver
    dummy_driver = get_url('https://www.google.com', dummy_driver)
    range_name = f'Sheet1!A2:E{row_count_max}'
    spreadsheet_id = url.split('/')[5]
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=range_name).execute()
    rows = result.get('values', [])
    return rows

def write_status(row_number, value, url):

    global dummy_driver
    dummy_driver = get_url('https://www.google.com', dummy_driver)

    credentials = get_Google_API_creds()
    try:
        sheets_service = build('sheets', 'v4', credentials=credentials, cache_discovery=False, static_discovery=False)
    except Exception as err:
      DISCOVERY_SERVICE_URL = 'https://sheets.googleapis.com/'
      sheets_service = build('sheets', 'v4', credentials=credentials, discoveryServiceUrl=DISCOVERY_SERVICE_URL, cache_discovery=False, static_discovery=False)

    spreadsheet_id = url.split('/')[5]
    range_name = f'Sheet1!B{row_number}:B{row_number}'
    value_input_option = 'USER_ENTERED'
    body = {'values': [[value]]}
    dummy_driver = get_url('https://www.google.com', dummy_driver)
    sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, range=range_name,
        valueInputOption=value_input_option, body=body).execute()

def get_url(url, driver):

    err_msg = False
    while True:
        try:
            driver.get(url)
            if err_msg:
                output_msg('The internet connection is restored, resuming the bot ...', 1)
                output_msg('-'*75, 0)
            break
        except Exception as err:
            if not err_msg:
                output_msg('Warning: failed to load a page, waiting for the internet connection ...', 1)
                err_msg = True
            time.sleep(60)

    return driver

def set_calendar(driver, month, order):

    # setting check-in and check-out dates
    try:
        div = wait(driver, 2).until(EC.presence_of_all_elements_located((By.XPATH, "//div[@class='Wvne hh3Grb']/div")))[-2]
    except Exception as err:
        div = wait(driver, 2).until(EC.presence_of_all_elements_located((By.XPATH, "//div[@jscontroller='OKD1oe']")))[-2]
    try:
        div.click()
    except Exception as err:
        pass
    time.sleep(1)
    div = wait(driver, 2).until(EC.presence_of_element_located((By.XPATH, "//div[@class='XfpsVe J9fJmf']")))
    buttons = wait(div, 2).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "button[class='VfPpkd-LgbsSe VfPpkd-LgbsSe-OWXEXe-dgl2Hf ksBjEc lKxP2d LQeN7 bRx3h yJQRU sIWnMc']")))
    reset = False
    for button in buttons:
        if button.get_attribute('textContent') == 'Reset':
            driver.execute_script("arguments[0].click();", button)
            time.sleep(1)
            reset = True
            break

    if not reset:
        buttons = wait(driver, 2).until(EC.presence_of_all_elements_located((By.TAG_NAME, "button")))
        for button in buttons:
            if button.get_attribute('textContent') == 'Reset':
                driver.execute_script("arguments[0].click();", button)
                time.sleep(1)
                reset = True
                break

    if not reset:
        output_msg(f"Warning: Failed to reset the calendar for {month}", 0)   

    j = 0
    while True:
        month22 = wait(driver, 2).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div[role='rowgroup']")))[j]
        if month in month22.get_attribute('textContent'):
            days = wait(month22, 2).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div[class='eoY5cb CylAxb sLG56c yCya5']")))
            for day in days:
                if day.get_attribute('textContent') == '1':
                    day.click()
                    time.sleep(1)
                    continue
                elif day.get_attribute('textContent') == '2':
                    day.click()
                    time.sleep(1)
                    # closing the calendar
                    div = wait(driver, 2).until(EC.presence_of_element_located((By.XPATH, "//div[@class='XfpsVe J9fJmf']")))
                    buttons = wait(div, 2).until(EC.presence_of_all_elements_located((By.TAG_NAME, "button")))
                    for button in buttons:
                        if button.get_attribute('textContent') == 'Done':
                            driver.execute_script("arguments[0].click();", button)
                            time.sleep(1)
                            break
                    break
            break
        else:
            # moving to the next month
            div = wait(driver, 2).until(EC.presence_of_all_elements_located((By.XPATH, "//div[@class='d53ede rQItBb FfP4Bc Gm3csc']")))[order]
            wait(div, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "button[aria-label='Next']"))).click()
            time.sleep(0.5)
            j += 1

    return driver

def search_hotels(url, dest, k, prop_type, prop_class, rating, min_price, max_price, curr, csv_limit, date1, date2, date3, date4):

    global dummy_driver, driver_path
    start = time.time()
    output_msg(f'Scraping destination: {dest[0].title()} ...', 1)
    # output path for the csv file
    dest_name = dest[0].title().replace(',', '_').replace(' ', '_')
    path = os.getcwd() + '\\csv_files'
    # datastructure
    df = pd.DataFrame()
    hotel = {}
    urls = []

    driver = initialize_bot(driver_path)
    months = [date1, date2, date3, date4]
    # checking rentals websites exclusion list
    if prop_type == 'Holiday rentals':
        if os.path.isfile(os.getcwd() + f"\\Rentals_Exclusion.csv"):
            df2 = pd.read_csv(os.getcwd() + f"\\Rentals_Exclusion.csv")
            exclusion = df2['Website'].unique().tolist()
        else:
            output_msg(f'Error: Missing Rentals Websites Exclusion file "Rentals_Exclusion.csv". Exiting The Bot ...', 1)
            sys.exit(1)

    symbol = CurrencySymbols.get_symbol(curr)
    # destination name is given
    if 'https://' not in dest[0].lower() and 'http://' not in dest[0].lower() and 'www.' not in dest[0].lower():
        # navigating to google travel
        #driver.get('https://www.google.com/travel/hotels')
        for _ in range(3):
            try:
                driver.get('https://www.google.com/travel/hotels')
                time.sleep(2)
                # sending dummy text to initialize the textbox class
                search = wait(driver, 60).until(EC.presence_of_element_located((By.XPATH, "//input[@class='II2One j0Ppje zmMKJ LbIaRd']")))
                search.send_keys(' ')
                time.sleep(1)
                # sending the destination
                search = wait(driver, 60).until(EC.presence_of_element_located((By.XPATH, "//input[@class='II2One j0Ppje zmMKJ LbIaRd']")))
                search.clear()
                search.send_keys(dest[0])
                time.sleep(1)
                search.send_keys(Keys.ENTER)
                time.sleep(5)      
                link = driver.current_url
                break
            except Exception as err:
                driver.quit()
                time.sleep(3)
                driver = initialize_bot(driver_path)

    # destination url is given
    elif 'https://www.google.com/travel/hotels' in dest[0]:
        #driver.get(dest[0])
        driver = get_url(dest[0], driver)
        time.sleep(2)
        link = driver.current_url
        search = wait(driver, 2).until(EC.presence_of_element_located((By.XPATH, "//input[@class='II2One j0Ppje zmMKJ LbIaRd']")))
        # updating the destination name 
        dest_name = search.get_attribute('value').split(' in ')[-1].strip().title()
        temp = [dest_name]
        m = len(dest)
        for i in range(1, m):
            temp.append(dest[i])
        dest = tuple(temp)
    else:
        write_status(k+2, 'Invalid Destination', url)
        output_msg(f'Warning: Invalid Destination input - {dest[0].title()}', 1)
        return df, dest

    try:
        # currency filter
        dummy_driver = get_url('https://www.google.com', dummy_driver)
        output_msg(f'Applying the currency filter: {curr}', 1)
        button = wait(driver, 2).until(EC.presence_of_element_located((By.XPATH, "//button[@class='VfPpkd-LgbsSe VfPpkd-LgbsSe-OWXEXe-INsAgc VfPpkd-LgbsSe-OWXEXe-Bz112c-UbuQg Rj2Mlf OLiIxf PDpWxe LQeN7 CapH0e Vdm71b']"))) 
        driver.execute_script("arguments[0].click();", button)
        time.sleep(5)

        options = wait(driver, 2).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "label.e2lIRb"))) 
        for opt in options:
            if curr in opt.get_attribute('textContent'):
                dummy_driver = get_url('https://www.google.com', dummy_driver)
                div_tag = wait(opt, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.zJKIV.k11Tnd.WEM9De.i9xfbb"))) 
                driver.execute_script("arguments[0].click();", div_tag)
                time.sleep(1)
                buttons = wait(driver, 2).until(EC.presence_of_all_elements_located((By.XPATH, "//button[@data-id='iib5kc']"))) 
                for button in buttons:
                    if 'Done' in button.get_attribute('textContent'):
                        dummy_driver = get_url('https://www.google.com', dummy_driver)
                        driver.execute_script("arguments[0].click();", button)
                        break
                time.sleep(3)
                break 
    except Exception as err:
        output_msg(f'Error: failed to set the currency filter for destination {dest[0]}', 0)

    # for handling destinations:
    if '/entity/' not in link:
        # applying the required filters
        output_msg(f'Applying the property type filter: {prop_type}', 0)
        try:
            # property type 
            if prop_type == 'Hotels':
                button = wait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "button[id='8']")))
                dummy_driver = get_url('https://www.google.com', dummy_driver)
                driver.execute_script("arguments[0].click();", button)
                time.sleep(3)
            else:
                dummy_driver = get_url('https://www.google.com', dummy_driver)
                button = wait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "button[id='14']")))
                driver.execute_script("arguments[0].click();", button)
                time.sleep(3)    
        except Exception as err:
            output_msg(f'Error: failed to set the property type filter for destination {dest[0]}', 0)
   
        # property class
        output_msg(f'Applying the class filter: {prop_class}', 0)
        try:
            if 'Any' not in prop_class:
                buttons = wait(driver, 2).until(EC.presence_of_all_elements_located((By.XPATH, "//button[@class='VfPpkd-LgbsSe VfPpkd-LgbsSe-OWXEXe-INsAgc VfPpkd-LgbsSe-OWXEXe-Bz112c-M1Soyc VfPpkd-LgbsSe-OWXEXe-dgl2Hf Rj2Mlf OLiIxf PDpWxe LQeN7 nCOOmf WAWozd J1HS6b']"))) 
                for button in buttons:
                    if 'Hotel class' in button.get_attribute('textContent'):
                        dummy_driver = get_url('https://www.google.com', dummy_driver)
                        driver.execute_script("arguments[0].click();", button)
                        time.sleep(3)
                        break

                div = wait(driver, 2).until(EC.presence_of_element_located((By.XPATH, "//div[@class='LNeN9 h3FxD']"))) 
                options = wait(div, 2).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.FgDwhd"))) 
                for elem in prop_class:
                    for opt in options:
                        if elem in opt.get_attribute('textContent').split('\n')[0]:
                            input_tag = wait(opt, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "input.i0VPBe.AkNlI"))) 
                            dummy_driver = get_url('https://www.google.com', dummy_driver)
                            driver.execute_script("arguments[0].click();", input_tag)
                            time.sleep(3)
                            break

                # closing the class window
                button = wait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "button.VfPpkd-Bz112c-LgbsSe.yHy1rc.eT1oJ.mN1ivc.evEd9e.HJuSVb"))) 
                dummy_driver = get_url('https://www.google.com', dummy_driver)
                driver.execute_script("arguments[0].click();", button)
                time.sleep(3)
        except Exception as err:
            output_msg(f'Error: failed to set the property class filter for destination {dest[0]}', 0)
            
        # property guest rating
        output_msg(f'Applying the guest rating filter: {rating}', 0)
        if rating != 'Any':
            try:
                buttons = wait(driver, 2).until(EC.presence_of_all_elements_located((By.XPATH, "//button[@class='VfPpkd-LgbsSe VfPpkd-LgbsSe-OWXEXe-INsAgc VfPpkd-LgbsSe-OWXEXe-Bz112c-M1Soyc VfPpkd-LgbsSe-OWXEXe-dgl2Hf Rj2Mlf OLiIxf PDpWxe LQeN7 nCOOmf WAWozd J1HS6b']"))) 
                for button in buttons:
                    if 'Guest rating' in button.get_attribute('textContent'):
                        dummy_driver = get_url('https://www.google.com', dummy_driver)
                        driver.execute_script("arguments[0].click();", button)
                        time.sleep(3)
                        break

                div = wait(driver, 2).until(EC.presence_of_element_located((By.XPATH, "//div[@class='LNeN9 AL18ce uyNugc']"))) 
                options = wait(div, 2).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.FgDwhd.QB2Jof"))) 
                for opt in options:
                    if rating in opt.get_attribute('textContent').split('\n')[0]:
                        input_tag = wait(opt, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "input.i0VPBe.cq1co"))) 
                        dummy_driver = get_url('https://www.google.com', dummy_driver)
                        driver.execute_script("arguments[0].click();", input_tag)
                        time.sleep(1)
                        button = wait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "button.VfPpkd-Bz112c-LgbsSe.yHy1rc.eT1oJ.mN1ivc.evEd9e.HJuSVb"))) 
                        dummy_driver = get_url('https://www.google.com', dummy_driver)
                        driver.execute_script("arguments[0].click();", button)
                        time.sleep(3)
                        break
            except Exception as err:
                output_msg(f'Error: failed to set the guest rating filter for destination {dest[0]}', 0)

        try:
            # price filter
            if min_price == '':
                min_label = 'Any'
                min_price = -1
            else:
                min_label = min_price    
            if max_price == '':
                max_price = -1
                max_label = 'Any'
            else:
                max_label = max_price
            output_msg(f'Applying the price filter: {min_label} - {max_label} ({curr})', 0)
            if min_price != -1 or max_price != -1:
                buttons = wait(driver, 2).until(EC.presence_of_all_elements_located((By.XPATH, "//button[@class='VfPpkd-LgbsSe VfPpkd-LgbsSe-OWXEXe-INsAgc VfPpkd-LgbsSe-OWXEXe-Bz112c-M1Soyc VfPpkd-LgbsSe-OWXEXe-dgl2Hf Rj2Mlf OLiIxf PDpWxe LQeN7 nCOOmf WAWozd J1HS6b']"))) 
                for button in buttons:
                    if 'Price' in button.get_attribute('textContent'):
                        dummy_driver = get_url('https://www.google.com', dummy_driver)
                        driver.execute_script("arguments[0].click();", button)
                        time.sleep(3)
                        break

                # updating the min price
                if min_price > 0 and min_price!= -1:
                    node1 = wait(driver, 2).until(EC.presence_of_all_elements_located((By.XPATH, "//div[@class='VfPpkd-UTM9ec-tmWYNe']")))[0]
                    while True:
                        dummy_driver = get_url('https://www.google.com', dummy_driver)
                        ActionChains(driver).drag_and_drop_by_offset(node1, 10, 0).perform()
                        #time.sleep(0.1)
                        price1 = wait(driver, 2).until(EC.presence_of_all_elements_located((By.XPATH, "//span[@class='VfPpkd-MIfjnf-uDEFge-fmcmS']")))[0].get_attribute('textContent')
                        # removing non numeric digits
                        price1 = int(re.findall("\d+", price1)[0])
                        if price1 == min_price or (min_price > price1 - 5 and min_price < price1 + 5):
                            break    
       
                # updating the max price
                price2 = wait(driver, 2).until(EC.presence_of_all_elements_located((By.XPATH, "//span[@class='VfPpkd-MIfjnf-uDEFge-fmcmS']")))[1].get_attribute('textContent')
                # removing non numeric digits
                price2 = int(re.findall("\d+", price2)[0])
                if max_price < price2 and max_price != -1:
                    node2 = wait(driver, 2).until(EC.presence_of_all_elements_located((By.XPATH, "//div[@class='VfPpkd-UTM9ec-tmWYNe']")))[1]
                    while True:
                        dummy_driver = get_url('https://www.google.com', dummy_driver)
                        ActionChains(driver).drag_and_drop_by_offset(node2, -5, 0).perform()
                        #time.sleep(1)
                        price2 = wait(driver, 2).until(EC.presence_of_all_elements_located((By.XPATH, "//span[@class='VfPpkd-MIfjnf-uDEFge-fmcmS']")))[1].get_attribute('textContent')
                        # removing non numeric digits
                        price2 = int(re.findall("\d+", price2)[0])
                        if price2 == max_price or (max_price > price2 - 5 and max_price < price2 + 5):
                            break

                # closing the price window
                button = wait(driver, 2).until(EC.presence_of_element_located((By.XPATH, "//button[@class='VfPpkd-Bz112c-LgbsSe yHy1rc eT1oJ mN1ivc evEd9e HJuSVb']"))) 
                driver.execute_script("arguments[0].click();", button)
                time.sleep(5)
        except Exception as err:
            output_msg(f'Error: failed to set the price filter for destination {dest[0]}', 0)
 
        # setting the calendar to the first month
        for m, month in enumerate(months):
            if month == 'Disable': continue
            try:
                output_msg(f'Setting the calendar to {month}', 0)
                driver = set_calendar(driver, month, 0)
                break
            except Exception as err:
                pass

        output_msg(f'Getting the full {prop_type} list ...', 1)
        urls = []
        try:
            while True:
                hotels = wait(driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.PVOOXe")))
                for hotel in hotels:
                    try:
                        urls.append(hotel.get_attribute('href'))
                    except Exception as err:
                        pass

                if csv_limit != '':
                    if len(urls) > csv_limit*2: break

                button = wait(driver,  5).until(EC.presence_of_element_located((By.CSS_SELECTOR, "button[jsname='OCpkoe']")))
                driver.execute_script("arguments[0].click();", button)
                time.sleep(5)
        except Exception as err:
            # scrolling to the end of the page (lazy loading)
            while True:  
                try:
                    height1 = driver.execute_script("return document.body.scrollHeight")
                    driver.execute_script(f"window.scrollTo(0, {height1})")
                    time.sleep(1)
                    for _ in range(5):
                        htmlelement= wait(driver, 2).until(EC.presence_of_element_located((By.TAG_NAME, "html")))
                        htmlelement.send_keys(Keys.UP)
                        time.sleep(0.2)
                    time.sleep(2) 
                    height2 = driver.execute_script("return document.body.scrollHeight")
                    if csv_limit != '':
                        hotels = wait(driver, 2).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.PVOOXe")))
                        if len(hotels) == csv_limit*2: 
                            break
                    if int(height2) == int(height1):
                        break
                except Exception as err:
                    break

        if not urls:
            
            # getting the hotels urls
            try:
                hotels = wait(driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.PVOOXe")))
            except Exception as err:
                write_status(k+2, 'No Results', url)
                output_msg('No Results Found', 0)
                return df, dest

            output_msg(f'Getting {prop_type} urls for: {dest[0].title()}', 0)
            divs = wait(driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div[jsname='mutHjb']"))) 

            for i, elem in enumerate(hotels):
                # excluding vacation rentals when presented in hotels
                if  'RENTAL' in divs[i].get_attribute('textContent') and prop_type == 'Hotels': 
                    continue
                url = elem.get_attribute('href')
                if url not in urls:
                    urls.append(url)  
                if csv_limit != '':
                    if len(urls) == csv_limit*2: break
    else:
        # single hotel is given
        urls.append(driver.current_url)
        
    nhotels = len(urls)
    scraped = []
    ihotel = 0
    if csv_limit != '':
        if nhotels > csv_limit:
            limit = csv_limit
        else:
            limit = nhotels
    else:
        limit = nhotels
    # scraping the hotels datails
    for url in urls:
        #url = 'https://www.google.com/travel/search?q=Chicago&qs=CAEgACgAMiRDaGNJNzgtbHVvWGRob2FJQVJvS0wyMHZNREoyY0dkbk1SQUI4DUgA&ts=CAESCgoCCAMKAggDEAAaXAo-EjoKCC9tLzAxX2Q0MiUweDg4MGUyYzNjZDBmNGNiZWQ6MHhhZmUwYTZhZDA5YzBjMDAwOgdDaGljYWdvGgASGhIUCgcI5w8QBxgBEgcI5w8QBxgCGAEyAggBKg8KBToDVVNEGgAiBAoCEC4&ap=MAG6AQhvdmVydmlldw'
        try:
            driver = get_url(url, driver)
            hotel = {}
            for _ in range(3):
                try:
                    # moving to the overview button
                    button = wait(driver, 2).until(EC.presence_of_element_located((By.XPATH, "//div[@id='overview']")))
                    dummy_driver = get_url('https://www.google.com', dummy_driver)
                    driver.execute_script("arguments[0].click();", button)
                    time.sleep(1)
                    break
                except Exception as err:
                    driver.refresh()
                    time.sleep(2)

            # hotel location
            if nhotels > 1:
                # destination
                loc = dest[0].split(',')[0]
            else:
                # single hotel
                loc = dest[0].split(',')[-1]
            hotel['Location'] = loc.strip()

            # scraping hotel name
            name = ''
            try:
                try:
                    name = wait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "h1[jsname='Xmv8Ce']"))).get_attribute('textContent')
                except Exception as err:
                    name = wait(driver, 2).until(EC.presence_of_element_located((By.TAG_NAME, "h1"))).get_attribute('textContent')
                name = unidecode.unidecode(name)
            except Exception as err:
                pass

            # no valid name is scraped for the hotel
            if name == '': continue
            hotel['Name'] = name
            # if hotel is already scraped
            if name in scraped: continue
            output_msg(f'Scraping details of {prop_type[:-1]} {ihotel+1}/{limit}: {name} ..', 1)            
            #output_msg(f'link: {url}', 0)            
            hotel['Google Travel Link'] = driver.current_url
            # scraping hotel stars and address
            hotel['Star Rating'] = ''
            hotel['Address'] = ''
            hotel['Phone Number'] = ''
            try:
                info_sec = wait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.OGAsq")))
                div = wait(info_sec, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.fnmyY")))
                spans = wait(div, 2).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "span[class='CFH2De']")))
                nspans = len(spans)
                if nspans == 3:
                    hotel['Star Rating'] = spans[0].get_attribute('textContent')
                    add = spans[1].get_attribute('textContent')
                    add = unidecode.unidecode(add)
                    hotel['Address'] = add
                    hotel['Phone Number'] = spans[2].get_attribute('textContent')
                elif nspans == 2:
                    if '-star hotel' not in spans[0].get_attribute('textContent'):
                        add = spans[0].get_attribute('textContent')
                        add = unidecode.unidecode(add)
                        hotel['Address'] = add
                        hotel['Phone Number'] = spans[1].get_attribute('textContent')
                    else:
                        hotel['Star Rating'] = spans[0].get_attribute('textContent')
                        add = spans[1].get_attribute('textContent')
                        add = unidecode.unidecode(add)
                        hotel['Address'] = add

                elif nspans == 1:
                    if '-star hotel' not in spans[0].get_attribute('textContent'):
                        add = spans[0].get_attribute('textContent')
                        add = unidecode.unidecode(add)
                        hotel['Address'] = add
                    else:
                        hotel['Star Rating'] = spans[0].get_attribute('textContent')

                # telepone formatting
                if hotel['Phone Number'] != '':
                    tel = hotel['Phone Number']
                    # changing the format to the desired one
                    digits = tel.split(' ')
                    for dig in digits:
                        if '+' in dig:
                            tel = ' '.join(digits[1:])
                            break

                    chars = []
                    for char in tel:
                        if char.isnumeric():
                            chars.append(char)
                        else:
                            if char == '-' or char == '.'or char == '('or char == ')':
                                chars.append(' ')
                            elif char == ' ':
                                chars.append('-')
                    tel = ''.join(chars)
                    hotel['Phone Number'] = tel

                # address formatting
                if hotel['Address'] != '': 
                    add = hotel['Address']
                    # removing non ASCII characters from the address
                    add =  re.sub(r'[^\x00-\x7f]',r'', add) 
                    add = add.replace('.', ' ')
                    # removing extra spaces
                    add = " ".join(add.split())
                    # removing country from address
                    add = add.replace('/', ' ').replace('-', ' ')
                    add_elems = add.split(',')
                    updated_add = []
                    for elem in add_elems:
                        trim = False
                        country_name = ''
                        for country in pycountry.countries:
                            if country.name in elem:
                                trim = True
                                country_name = country.name
                                break                   

                        if trim:
                            text = elem.replace(country_name, '').strip()
                            if text != '':
                                updated_add.append(text)
                        else:
                            updated_add.append(elem)
                        
                    # removing postal code from address
                    if len(updated_add) > 0:
                        add_elems = updated_add[::-1]
                    else:
                        add_elems = [add]
                    updated_add = []
                    trim = False
                    n = len(add_elems)
                    for i, elem in enumerate(add_elems):
                        if not trim:
                            section = []
                            parts = elem.split(' ')
                            for j, part in enumerate(parts):
                                if part.isalpha() or part == ' ' or part == '' or "'" in part or (j == 0 and i == n-1):
                                    section.append(part)
                                else:
                                    trim = True
                            updated_add.append(' '.join(section))
                        else:
                            updated_add.append(elem)

                    add = ''.join(updated_add[::-1])
                    add = add.replace(',', '')

                    # further format updates
                    if add.find('Road') > 0:
                        add = add.replace('Road', 'Rd.') 
                    elif add.find('Rd.') > 0:
                        add = add.replace('Rd.', 'Road')
                    elif add.find('Rd') > 0:
                        add = add.replace('Rd', 'Road')

                    if add.find('Street') > 0:
                        add = add.replace('Street', 'St') 
                    elif add.find('St.') > 0:
                        add = add.replace('St.', 'Street')
                    elif add.find('St,') > 0:
                        add = add.replace('St,', 'Street')

                    hotel['Address'] = add
            except Exception as err:
                pass

            # scraping hotel website
            hotel['Website'] = ''
            try:
                dummy_driver = get_url('https://www.google.com', dummy_driver)
                hotel['Website'] = wait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "a[aria-label*='Visit site']"))).get_attribute('href')

                # formatting the website
                if hotel['Website'] != '':
                    site = hotel['Website']
                    # removing what is after the "?" in the link
                    if site.find('?') > 0:
                        site = site[:site.find('?')]
                    # removing "www." from the link   
                    site = site.replace('www.', '').replace('WWW.', '')
                    hotel['Website'] = site
            except Exception as err:
                pass

            hotel['Google Maps Location'] = ''
            if hotel['Star Rating'] != '':
                output_msg(f"Star Rating: {hotel['Star Rating']}", 0)
            if hotel['Address'] != '':
                output_msg(f"Address: {hotel['Address']}", 0)
            hotel['Short Description'] = ''
            try:
                # scraping hotel description
                dummy_driver = get_url('https://www.google.com', dummy_driver)
                des = wait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.y3yqve.QB2Jof"))).get_attribute('textContent')
                des = unidecode.unidecode(des)
                hotel['Short Description'] = des
            except Exception as err:
                pass
            # scraping hotel location rating and neighborhood
            hotel['Location Rating'] = ''
            try:
                dummy_driver = get_url('https://www.google.com', dummy_driver)
                loc_sec = wait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.Rcnomd")))
                loc_rating = wait(loc_sec, 2).until(EC.presence_of_element_located((By.TAG_NAME, "text"))).get_attribute('textContent').strip()
                hotel['Location Rating'] = float(loc_rating)*2
            except Exception as err:
                pass

            output_msg(f"Location Rating: {hotel['Location Rating']}", 0)
            nei = ''
            try:
                dummy_driver = get_url('https://www.google.com', dummy_driver)
                nei = wait(loc_sec, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "span[class='CQYfx']"))).get_attribute('textContent')
                nei = unidecode.unidecode(nei)
            except Exception as err:
                pass
            hotel['Neighbourhood'] = nei

            loc_des = ''
            try:
                dummy_driver = get_url('https://www.google.com', dummy_driver)
                loc_des = wait(loc_sec, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "span[class='CQYfx ivkG9b']"))).get_attribute('textContent')
                loc_des = unidecode.unidecode(loc_des)
            except Exception as err:
                pass
            hotel['Location Description'] = loc_des
       
            #moving to prices tab
            button = wait(driver, 2).until(EC.presence_of_element_located((By.XPATH, "//div[@id='prices']")))
            dummy_driver = get_url('https://www.google.com', dummy_driver)
            driver.execute_script("arguments[0].click();", button)
            time.sleep(1)
            
            for month in months:
                if month == 'Disable': continue
                hotel[f'{month} Price'] = ''

            for m, month in enumerate(months):
                if month == 'Disable': continue
                try:
                    # check if the calendar is set on the right month
                    checkin_date = wait(driver, 2).until(EC.presence_of_element_located((By.XPATH, "//input[@aria-label='Check-in']"))).get_attribute('value')
                    checkout_date = wait(driver, 2).until(EC.presence_of_element_located((By.XPATH, "//input[@aria-label='Check-out']"))).get_attribute('value')
                    if f"{month[:3]} 1" not in checkin_date or f"{month[:3]} 2" not in checkout_date: 
                        driver = set_calendar(driver, month, -1)

                    # getting the prices
                    htmlelement= wait(driver, 2).until(EC.presence_of_element_located((By.TAG_NAME, "html")))
                    htmlelement.send_keys(Keys.END)
                    try:
                        hotel_prices = []
                        # for hotels
                        try:                  
                             hotel_prices = wait(driver, 1).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "span[class='pNExyb']")))
                        except Exception as err:
                            pass

                        if not hotel_prices:
                            # for rentals 1
                            try:
                                hotel_prices = wait(driver, 1).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div[class='xNhYwc']")))
                            except Exception as err:
                               pass                        
      
                        if not hotel_prices:
                            # for rentals 2
                            try:
                                hotel_prices = wait(driver, 1).until(EC.presence_of_all_elements_located((By.XPATH, "//div[@class='QC5IW']")))
                            except Exception as err:
                               pass                       
       
                        if not hotel_prices:
                            # single price
                            try:
                                hotel_prices = wait(driver, 1).until(EC.presence_of_all_elements_located((By.XPATH, "//span[@class='QC5IW']")))
                            except Exception as err:
                               pass

                       # calculating min and max prices
                        min_price, max_price = 1e10, -1
                        for price in hotel_prices:
                            try:
                                price = price.get_attribute('textContent').replace(',', '').split('$')[1]
                                try:
                                    price = float(re.findall("[0-9]+[.][0-9]+", price)[0])
                                except Exception as err:
                                    price = float(re.findall("[0-9]+", price)[0])
                                #output_msg(price, 0) 
                                min_price = min(min_price, price)
                                max_price = max(max_price, price)
                            except Exception as err:
                                continue
                        hotel[f'{month} Min Price'] = min_price
                        hotel[f'{month} Max Price'] = max_price
  
                    except Exception as err:
                        try:
                            dummy_driver = get_url('https://www.google.com', dummy_driver)
                            price = wait(driver, 2).until(EC.presence_of_all_elements_located((By.XPATH, "//span[@class='QC5IW']")))[-1].get_attribute('textContent')
                            try:
                                price = float(re.findall("[0-9]+[.][0-9]+", price)[0])
                            except Exception as err:
                                price = float(re.findall("[0-9]+", price)[0])

                            hotel[f'{month} Min Price'] = price 
                            hotel[f'{month} Max Price'] = price       
                        except Exception as err:
                            hotel[f'{month} Min Price'] = ''
                            hotel[f'{month} Max Price'] = ''  
                except Exception as err:
                    output_msg(f"The below error occurred while scraping {month} prices:\n", 0) 
                    output_msg(str(err), 0) 


            # sanity check 
            for month in months:
                if month == 'Disable': continue
                try:
                    if hotel[f'{month} Min Price'] == 1e10:
                        price = wait(driver, 1).until(EC.presence_of_all_elements_located((By.XPATH, "//span[@class='QC5IW']")))[-1].get_attribute('textContent')
                        try:
                            price = float(re.findall("[0-9]+[.][0-9]+", price)[0])
                        except Exception as err:
                            price = float(re.findall("[0-9]+", price)[0])

                        hotel[f'{month} Min Price'] = price 
                        hotel[f'{month} Max Price'] = price 
                        
                    output_msg(f"Min price for {month}: {hotel[f'{month} Min Price']}{symbol}", 0)
                except Exception as err:
                    hotel[f'{month} Min Price'] = ''
                   
                try:
                    if hotel[f'{month} Max Price'] == -1:
                        price = wait(driver, 1).until(EC.presence_of_all_elements_located((By.XPATH, "//span[@class='QC5IW']")))[-1].get_attribute('textContent')
                        price = wait(driver, 1).until(EC.presence_of_all_elements_located((By.XPATH, "//span[@class='QC5IW']")))[-1].get_attribute('textContent')
                        try:
                            price = float(re.findall("[0-9]+[.][0-9]+", price)[0])
                        except Exception as err:
                            price = float(re.findall("[0-9]+", price)[0])

                        hotel[f'{month} Min Price'] = price 
                        hotel[f'{month} Max Price'] = price
                        
                    output_msg(f"Max price for {month}: {hotel[f'{month} Max Price']}{symbol}", 0)
                except Exception as err:
                    hotel[f'{month} Max Price'] = ''

            for _ in range(5):
                try:
                    # moving to reviews tab
                    button = wait(driver, 2).until(EC.presence_of_element_located((By.XPATH, "//div[@id='reviews']")))
                    dummy_driver = get_url('https://www.google.com', dummy_driver)
                    driver.execute_script("arguments[0].click();", button)
                    time.sleep(1)
                    htmlelement= wait(driver, 2).until(EC.presence_of_element_located((By.TAG_NAME, "html")))
                    htmlelement.send_keys(Keys.END)
                    break
                except Exception as err:
                    driver.refresh()
                    time.sleep(1)

            # scraping guest rating and rating word
            hotel['Guest Rating'] = ''
            try:
                dummy_driver = get_url('https://www.google.com', dummy_driver)
                guest_rating = wait(driver, 2).until(EC.presence_of_element_located((By.XPATH, "//div[@class='FBsWCd']"))).get_attribute('textContent')
                hotel['Guest Rating'] = float(guest_rating)*2
            except Exception as err:
                try:
                    guest_rating = wait(driver, 2).until(EC.presence_of_element_located((By.XPATH, "//div[@class='BARtsb']"))).get_attribute('textContent')
                    hotel['Guest Rating'] = float(guest_rating)*2
                except Exception as err:
                    pass

            if hotel['Guest Rating'] != '':
                output_msg(f"Guest Rating: {hotel['Guest Rating']}", 0)   

            rating_word = ''
            try:
                dummy_driver = get_url('https://www.google.com', dummy_driver)
                try:
                    div = wait(driver, 2).until(EC.presence_of_element_located((By.XPATH, "//div[@class='U675cf sSHqwe']")))
                    rating_word = wait(div, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "span[class='QB2Jof']"))).get_attribute('textContent')
                except Exception as err:
                    rating_word = wait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[class='YMlIz']"))).get_attribute('textContent')
            except Exception as err:
                try:
                    rating_word = wait(driver, 2).until(EC.presence_of_element_located((By.XPATH, "//span[@class='QB2Jof']"))).get_attribute('textContent')
                except Exception as err:
                    pass

            hotel['Rating Word'] = rating_word

            if hotel['Rating Word'] != '':
                output_msg(f"Rating Word: {hotel['Rating Word']}", 0) 

            hotel['Number of Reviews'] = 0
            try:
                dummy_driver = get_url('https://www.google.com', dummy_driver)
                # scraping hotel number of reviews
                div = wait(driver, 2).until(EC.presence_of_element_located((By.XPATH, "//div[@class='d7ZjBe']")))
                nrev = wait(div, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "span[class='P2NYOe GFm7je sSHqwe']"))).get_attribute('textContent')
                nrev = int(re.sub("[^0-9]", "", nrev))
                hotel['Number of Reviews'] = nrev
            except Exception as err:
                try:
                    div = wait(driver, 2).until(EC.presence_of_element_located((By.XPATH, "//div[@class='FzERAb']")))
                    nrev = wait(div, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[class='sSHqwe']"))).get_attribute('textContent')
                    nrev = int(re.sub("[^0-9]", "", nrev))
                    hotel['Number of Reviews'] = nrev
                except Exception as err:
                    pass
            output_msg(f"Number of Reviews: {hotel['Number of Reviews']}", 0) 
            # scraping ratings percentages
            hotel['5 star Guest rating Percentage'] = ''
            hotel['4 star Guest rating Percentage'] = ''
            hotel['3 star Guest rating Percentage'] = ''
            hotel['2 star Guest rating Percentage'] = ''
            hotel['1 star Guest rating Percentage'] = ''
            try:
                dummy_driver = get_url('https://www.google.com', dummy_driver)
                div = wait(driver, 2).until(EC.presence_of_element_located((By.XPATH, "//div[@class='t02EL']")))
                spans = wait(div, 2).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div[class='dcdcEf UsMcpf']")))
                five_per = spans[0].get_attribute('aria-label').split()[-2]
                hotel['5 star Guest rating Percentage'] = five_per
                four_per = spans[1].get_attribute('aria-label').split()[-2]
                hotel['4 star Guest rating Percentage'] = four_per
                three_per = spans[2].get_attribute('aria-label').split()[-2]
                hotel['3 star Guest rating Percentage'] = three_per
                two_per = spans[3].get_attribute('aria-label').split()[-2]
                hotel['2 star Guest rating Percentage'] = two_per
                one_per = spans[4].get_attribute('aria-label').split()[-2]
                hotel['1 star Guest rating Percentage'] = one_per
            except Exception as err:
                pass
            if hotel['5 star Guest rating Percentage'] != '':
                output_msg(f"5 star Guest rating Percentage: {hotel['5 star Guest rating Percentage']}%", 0)
            if hotel['4 star Guest rating Percentage'] != '':
                output_msg(f"4 star Guest rating Percentage: {hotel['4 star Guest rating Percentage']}%", 0)
            if hotel['3 star Guest rating Percentage'] != '':
                output_msg(f"3 star Guest rating Percentage: {hotel['3 star Guest rating Percentage']}%", 0)
            if hotel['2 star Guest rating Percentage'] != '':
                output_msg(f"2 star Guest rating Percentage: {hotel['2 star Guest rating Percentage']}%", 0)
            if hotel['1 star Guest rating Percentage'] != '':
                output_msg(f"1 star Guest rating Percentage: {hotel['1 star Guest rating Percentage']}%", 0)

            try:
                dummy_driver = get_url('https://www.google.com', dummy_driver)
                div = wait(driver, 2).until(EC.presence_of_all_elements_located((By.XPATH, "//div[@class='xv9psc zLMShf PMuGec']")))[-1]
                names_spans = wait(div, 2).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "span[class='R9aG4b']")))
                n = len(names_spans)
                total_mentions = 0
                for k in range(n):
                    text = names_spans[k].get_attribute('textContent')
                    amen_name = text.split('(')[0]
                    hotel[f'{amen_name} Mention Count'] = ''
                    hotel[f'{amen_name} Mention out of Total Reviews'] = ''
                    try:
                        mentions = int(re.sub("[^0-9]", "", text))
                        total_mentions += mentions
                        hotel[f'{amen_name} Mention Count'] = mentions
                        hotel[f'{amen_name} Mention out of Total Reviews'] = round(mentions/hotel['Number of Reviews'], 2)
                    except Exception as err:
                        continue

                hotel['Total Amenities Mentions'] = total_mentions
                hotel['Amenities Mention Stregth'] = round(total_mentions/hotel['Number of Reviews'], 2)

            except Exception as err:
                pass


            try:
                dummy_driver = get_url('https://www.google.com', dummy_driver)
                # moving to overview tab
                button = wait(driver, 2).until(EC.presence_of_element_located((By.XPATH, "//div[@id='overview']")))
                dummy_driver = get_url('https://www.google.com', dummy_driver)
                driver.execute_script("arguments[0].click();", button)
                time.sleep(1)   
            except Exception as err:
                pass

            # moving to top sights menu
            output_msg(f"Scraping {prop_type[:-1]} top sights", 0)
            try:
                buttons = wait(driver, 2).until(EC.presence_of_all_elements_located((By.XPATH, "//div[@id='topsights']")))
                for button in buttons:
                    if 'Top sights' in button.get_attribute('textContent'):
                        dummy_driver = get_url('https://www.google.com', dummy_driver)
                        driver.execute_script("arguments[0].click();", button)
                        time.sleep(1)
                        break

                insights_menu = wait(driver, 2).until(EC.presence_of_element_located((By.XPATH, "//span[@id='topsights']")))
                insights = wait(insights_menu, 2).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div[class='ujJAIe']")))
                n = len(insights)
                if n > 9:
                    insights = insights[:9]
                for l, ins in enumerate(insights):
                    try:
                        name = wait(ins, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[class='AdWm1c AFZtd ogfYpf']"))).get_attribute('textContent').strip()
                        name = unidecode.unidecode(name) 
                        rating = ''
                        try:
                            rating = wait(ins, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "span[class='KFi5wf lA0BZ CQYfx']"))).get_attribute('textContent').strip()
                        except Exception as err:
                            pass      
                        des = ''
                        try:
                            des = wait(ins, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[class='sSHqwe bJlStd ogfYpf']"))).get_attribute('textContent').strip()    
                        except Exception as err:
                            pass
                        dist, trans = '', ''
                        try:
                            dist_info = wait(ins, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "span[class='veMtCf']"))).get_attribute('textContent').strip()
                            unit = dist_info.split(' ')[-1].strip()
                            dist = int(dist_info.split(' ')[0].strip())
                            # converting sec to min
                            if 'sec' in unit:
                                dist = dist/60
                            # capping the min distance at 5 mins
                            if  dist < 5:
                                dist = 5
            
                            text = wait(ins, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "span[class='kJW6fe']"))).get_attribute('aria-label')
                            trans = 'Drive'
                            if 'taxi' in text or 'drive' in text or 'bus' in text:
                                trans = 'Drive'
                            elif 'walk' in text:
                                trans = 'Walk'
                        except Exception as err:
                            pass

                        hotel[f'Top Sight{l+1} Name'] = name   
                        hotel[f'Top Sight{l+1} Rating'] = rating
                        hotel[f'Top Sight{l+1} Description'] = des
                        hotel[f'Top Sight{l+1} Distance in min'] = dist
                        hotel[f'Top Sight{l+1} Transportation'] = trans
                    except Exception as err:
                        pass
            except Exception as err:
                pass
            
            # moving to restaurants menu
            output_msg(f"Scraping {prop_type[:-1]} nearby restaurants", 0)
            try:
                buttons = wait(driver, 2).until(EC.presence_of_all_elements_located((By.XPATH, "//div[@id='restaurants']")))
                for button in buttons:
                    if 'Restaurants' in button.get_attribute('textContent'):
                        dummy_driver = get_url('https://www.google.com', dummy_driver)
                        driver.execute_script("arguments[0].click();", button)
                        time.sleep(1)
                        break

                res_menu = wait(driver, 2).until(EC.presence_of_element_located((By.XPATH, "//span[@id='restaurants']")))
                restaurants = wait(res_menu, 2).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div[class='ujJAIe']")))
                n = len(restaurants)
                if n > 9:
                    restaurants = restaurants[:9]
                ires = 0
                for res in restaurants:
                    try:
                        name = wait(res, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[class='AdWm1c AFZtd ogfYpf']"))).get_attribute('textContent').strip()
                        name = unidecode.unidecode(name)  
                        res_rating = ''
                        try:
                            res_rating = wait(res, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "span[class='KFi5wf lA0BZ CQYfx']"))).get_attribute('textContent').strip()
                        except Exception as err:
                            pass        
                        res_type = ''
                        try:
                            res_type = wait(res, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[class='sSHqwe bJlStd ogfYpf']"))).get_attribute('textContent').strip()
                        except Exception as err:
                            pass

                        dist, trans = '', ''
                        try:
                            dist_info = wait(res, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "span[class='veMtCf']"))).get_attribute('textContent').strip()
                            unit = dist_info.split(' ')[-1].strip()
                            dist = int(dist_info.split(' ')[0].strip())
                            # converting sec to min
                            if 'sec' in unit:
                                dist = dist/60
                            # capping the min distance at 5 mins
                            if  dist < 5:
                                dist = 5
            
                            text = wait(res, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "span[class='kJW6fe']"))).get_attribute('aria-label').lower()
                            trans = 'Drive'
                            if 'taxi' in text or 'drive' in text or 'bus' in text:
                                trans = 'Drive'
                            elif 'walk' in text:
                                trans = 'Walk'
                            ires += 1
                        except Exception as err:
                            pass

                        hotel[f'Restaurant{ires} Name'] = name
                        hotel[f'Restaurant{ires} Rating'] = res_rating                        
                        hotel[f'Restaurant{ires} Type'] = res_type                        
                        hotel[f'Restaurant{ires} Distance in min'] = dist
                        hotel[f'Restaurant{ires} Transportation'] = trans
                    except Exception as err:
                        pass

            except Exception as err:
                pass

            # moving to airports menu
            output_msg(f"Scraping {prop_type[:-1]} nearby airports", 0)
            try:
                dummy_driver = get_url('https://www.google.com', dummy_driver)
                buttons = wait(driver, 2).until(EC.presence_of_all_elements_located((By.XPATH, "//div[@id='airports']")))
                for button in buttons:
                    if 'Airports' in button.get_attribute('textContent'):
                        dummy_driver = get_url('https://www.google.com', dummy_driver)
                        driver.execute_script("arguments[0].click();", button)
                        time.sleep(1)
                        break

                airport_menu = wait(driver, 2).until(EC.presence_of_element_located((By.XPATH, "//span[@id='airports']")))
                airports = wait(airport_menu, 2).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div[class='ujJAIe']")))
                n = len(airports)
                if n > 2:
                    airports = airports[:2]  
                iair = 0
                for air in airports:
                    name = wait(air, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[class='AdWm1c AFZtd ogfYpf xfAZzb']"))).get_attribute('textContent').strip()
                    name = unidecode.unidecode(name)
                    dists_info = wait(air, 2).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div[class='rGu7td H3JEKf']")))
                    for dist_info in dists_info:
                        try:
                            trans = dist_info.get_attribute('textContent')
                            if 'Taxi' not in trans: continue
                            info = wait(dist_info, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[class='kC4Ofd NUiScc']"))).get_attribute('textContent').split()
                            if len(info) == 2:
                                dist = int(info[0].strip())
                                unit = info[1].strip()
                                # converting sec to min
                                if 'sec' in unit:
                                    dist = dist/60                    
                                # converting hour to min
                                if 'hr' in unit:
                                    dist = dist*60
                                # capping the min distance at 5 mins
                                if  dist < 5:
                                    dist = 5
                            else:
                                # time in hrs and mins
                                dist = int(info[0])*60 + int(info[2])
                            iair += 1
                            hotel[f'Airport{iair} Name'] = name
                            hotel[f'Airport{iair} Distance in min by Taxi'] = dist
                        except Exception as err:
                            pass
                    # clicking on the next airport
                    #button = wait(driver, 2).until(EC.presence_of_element_located((By.XPATH, "//div[@class='Td3vn']")))
                    #dummy_driver = get_url('https://www.google.com', dummy_driver)
                    #driver.execute_script("arguments[0].click();", button)
                    #time.sleep(1)
            except Exception as err:
                pass

            # moving to public Transit menu
            output_msg(f"Scraping {prop_type[:-1]} nearby public transportation", 0)
            try:
                buttons = wait(driver, 2).until(EC.presence_of_all_elements_located((By.XPATH, "//div[@id='transit']")))
                for button in buttons:
                    if 'Transit' in button.get_attribute('textContent'):
                        dummy_driver = get_url('https://www.google.com', dummy_driver)
                        driver.execute_script("arguments[0].click();", button)
                        time.sleep(2)
                        break

                transit_menu = wait(driver, 2).until(EC.presence_of_element_located((By.XPATH, "//span[@id='transit']")))
                transits = wait(transit_menu, 2).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div[class='ujJAIe']")))
                itrans = 0
                for trans in transits:
                    name = wait(trans, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[class='AdWm1c AFZtd ogfYpf']"))).get_attribute('textContent').strip()
                    name = unidecode.unidecode(name)
                    dists_info = wait(trans, 2).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div[class='rGu7td H3JEKf']")))
                    for dist_info in dists_info:
                        try:
                            info = wait(dist_info, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[class='kC4Ofd NUiScc']"))).get_attribute('textContent').split()
                            if len(info) == 2:
                                dist = int(info[0].strip())
                                unit = info[1].strip()
                                # converting sec to min
                                if 'sec' in unit:
                                    dist = dist/60                    
                                # converting hour to min
                                if 'hr' in unit:
                                    dist = dist*60
                                # capping the min distance at 5 mins
                                if  dist < 5:
                                    dist = 5
                            else:
                                # time in hrs and mins
                                dist = int(info[0])*60 + int(info[2])

                            text = dist_info.get_attribute('textContent').lower()
                            mode = 'Drive'
                            if 'taxi' in text or 'drive' in text or 'bus' in text:
                                mode = 'Drive'
                            elif 'walk' in text:
                                mode = 'Walk'

                            itrans += 1
                            hotel[f'Transit{itrans} Name'] = name
                            hotel[f'Transit{itrans} Distance in min'] = dist
                            hotel[f'Transit{itrans} Transportation'] = mode
                        except Exception as err:
                            pass
            except Exception as err:
                pass

            try:
                # moving to the about tab
                button = wait(driver, 2).until(EC.presence_of_element_located((By.XPATH, "//div[@id='details']")))
                dummy_driver = get_url('https://www.google.com', dummy_driver)
                driver.execute_script("arguments[0].click();", button)
                time.sleep(1)
            except Exception as err:
                pass

            output_msg(f"Scraping {prop_type[:-1]} description", 0)
            # scraping the hotel description
            about = ''
            try:
                dummy_driver = get_url('https://www.google.com', dummy_driver)
                section1 = wait(driver, 2).until(EC.presence_of_all_elements_located((By.XPATH, "//section[@class='mEKuwe G8T82']")))[0]
                divs = wait(section1, 2).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div[class='D35lie']")))
        
                # check for read more button
                try:
                    button = wait(driver, 2).until(EC.presence_of_element_located((By.XPATH, "//button[@aria-label='Read more']")))
                    dummy_driver = get_url('https://www.google.com', dummy_driver)
                    driver.execute_script("arguments[0].click();", button)
                    time.sleep(1)
                except Exception as err:
                    pass
                about = divs[0].get_attribute('textContent')
                about = about.replace('… Read more', '\n').strip()
                about = unidecode.unidecode(about)

                # No about paragraph
                if "Check-in time:" in about and "Check-out time:" in about:
                    about = ''
            except Exception as err:

                if not about:
                    try:
                        section1 = wait(driver, 2).until(EC.presence_of_all_elements_located((By.XPATH, "//section[@class='O3oTUb']")))[0]
                        about = section1.get_attribute('textContent')
                        about = about.replace('Details', '').strip()
                        about = unidecode.unidecode(about)
                    except Exception as err:
                        pass

            hotel['About'] = about

            # scraping check-in and check-out 
            checkin, checkout = '', ''
            try:
                divs = wait(section1, 2).until(EC.presence_of_all_elements_located((By.TAG_NAME, "div")))
                for div in divs:
                    text = div.get_attribute('textContent')
                    if 'Check-in time:' in text:
                        checkin = text.replace('Check-in time:', '').strip()
                        checkin = unidecode.unidecode(checkin)
                    elif 'Check-out time: ' in text:
                        checkout = text.replace('Check-out time:', '').strip()
                        checkout = unidecode.unidecode(checkout)
                    elif checkin != '' and checkout != '':
                        break
            except Exception as err:
                pass

            # for rentals
            if checkin == '' and checkout == '':
                try:
                    found = False
                    sections = wait(driver, 2).until(EC.presence_of_all_elements_located((By.TAG_NAME, "section")))
                    for sec in sections:
                        if found: break
                        try:
                            divs = wait(sec, 2).until(EC.presence_of_all_elements_located((By.TAG_NAME, "div")))
                            for div in divs:
                                text = div.get_attribute('textContent')
                                if 'Check-in time:' in text:
                                    checkin = text.replace('Check-in time:', '').strip()
                                    checkin = unidecode.unidecode(checkin)
                                elif 'Check-out time: ' in text:
                                    checkout = text.replace('Check-out time:', '').strip()
                                    checkout = unidecode.unidecode(checkout)
                                elif checkin != '' and checkout != '':
                                    found = True
                                    break
                        except Exception as err:
                            pass
                except Exception as err:
                    pass

            
            hotel['Check in Time'] = checkin
            hotel['Check Out Time'] = checkout

            if hotel['Check in Time'] != '':
                output_msg(f"Check in Time: {hotel['Check in Time']}", 0)
            if hotel['Check Out Time'] != '':
                output_msg(f"Check Out Time: {hotel['Check Out Time']}", 0)

            output_msg(f"Scraping {prop_type[:-1]} amenities ...", 0)

            # additional info for the rentals
            try:
                div = wait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[class='hwR8Dd DmBmM fmFs0c']")))
                if 'Essential info' in div.get_attribute('textContent'):
                    utils = wait(div, 2).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "span[class='veMtCf']")))
                    for util in utils:
                        try:
                            text = util.get_attribute('textContent').strip().title()
                            num = re.findall(r'[0-9]+' , text)
                            if num:
                                text = text.replace(num[0], '').strip().replace('  ', ' ')
                                hotel[text] = num[0]
                            else:
                                hotel[text] = 'Yes'     
                                
                            value = hotel[text]
                            output_msg(f"{text}: {value}", 0)
                        except Exception as err:
                            pass
            except Exception as err:
                pass

            # rentals listing type
            rental_types = ["lodge", "cottage", "motel", "hotel", "inn", "guesthouse", "hostel", "vacation rental", "apartment", "condominium", "villa", "resort", "homestay", "farmstay", "glamping", "campsite", "aparthotel", "townhouse", "bungalow", "chalet", "cabin", "yurt", "treehouse", "houseboat", "capsule hotel", "eco-lodge", "castle", "caravan park", "holiday park", "monastery stay", "bed and breakfast", "serviced apartment"]

            if 'hotel' in prop_type.lower():
                rental_type = 'Hotel'
            else:
                rental_type = 'Unknown'
            try:
                about = hotel['About'].lower()
                hotel_name = hotel['Name'].lower()
                for listing in rental_types:
                    if listing in about or listing in hotel_name:
                        rental_type = listing.title()
                        break
            except Exception as err:
                pass

            hotel['Listing Type'] = rental_type
            output_msg(f"Listing Type: {hotel['Listing Type']}", 0)

            try:
                sections = wait(driver, 1).until(EC.presence_of_all_elements_located((By.XPATH, "//section[@class='mEKuwe G8T82']")))[1:]
            except Exception as err:
                # for rentals
                sections = wait(driver, 1).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "section.O3oTUb")))[1:]

            for sec in sections:
                header = wait(sec, 1).until(EC.presence_of_element_located((By.TAG_NAME, "h2"))).get_attribute('textContent')
                if 'Health & safety' in header:
                    try:
                        try:
                            divs = wait(sec, 1).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div[jscontroller='N4VHee']")))
                        except Exception as err:
                            divs = wait(sec, 1).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div[class='V6LM8c']")))
                        for div in divs:
                            try:
                                title = wait(div, 1).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[jsname='sM5MNb']"))).get_attribute('textContent')
                            except Exception as err:
                                title = wait(div, 1).until(EC.presence_of_element_located((By.TAG_NAME, "h4"))).get_attribute('textContent')
                            try:
                                lis = wait(div, 1).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "li[class='ZQnR8e coK7uc']")))
                            except Exception as err:
                                lis = wait(div, 1).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "li")))
                            for l, li in enumerate(lis):
                                hotel[f"{title}{l+1}"] = li.get_attribute("textContent")
                    except Exception as err:
                        pass 
                
                elif 'Amenities' in header or 'Sustainability' in header:     
                    try:
                        dummy_driver = get_url('https://www.google.com', dummy_driver)
                        # popular amenities
                        try:
                            dummy_driver = get_url('https://www.google.com', dummy_driver)
                            popular = wait(sec, 1).until(EC.presence_of_element_located((By.XPATH, "//div[@class='KRM68c PnXVVe']")))
                            title = wait(popular, 1).until(EC.presence_of_element_located((By.TAG_NAME, "h4"))).get_attribute('textContent')
                            lis = wait(popular, 1).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "li[class='XX3dkb bsG9af']")))
                            for l, li in enumerate(lis):
                                hotel[f"{title}{l+1}"] = li.get_attribute("textContent").replace('free', '')
                        except Exception as err:
                            pass

                        #other amenities
                        try:
                            dummy_driver = get_url('https://www.google.com', dummy_driver)
                            amenities = wait(sec, 1).until(EC.presence_of_all_elements_located((By.XPATH, "//div[@class='IYmE3e']")))
                            for amen in amenities:
                                title = wait(amen, 1).until(EC.presence_of_element_located((By.TAG_NAME, "h4"))).get_attribute('textContent')
                                lis = wait(amen, 1).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "li[class='IXICF']")))
                                for l, li in enumerate(lis):
                                    span = wait(li, 1).until(EC.presence_of_element_located((By.CSS_SELECTOR, "span[class='LtjZ2d']"))).get_attribute('textContent')
                                    hotel[f"{title}{l+1}"] = span
                        except Exception as err:
                            pass

                    except Exception as err:
                        pass

                    # rentals amenities
                    try:
                        dummy_driver = get_url('https://www.google.com', dummy_driver)
                        amenities = wait(sec, 1).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "span[class='ZhP54c']")))
                        namen = 0
                        for amen in amenities:
                            span = amen.get_attribute('textContent').strip()
                            if span[:2] == 'No': continue
                            namen += 1
                            hotel[f"Amenity{namen}"] = span
                    except Exception as err:
                        pass

            for _ in range(5):
                try:
                    dummy_driver = get_url('https://www.google.com', dummy_driver)
                    # moving to photos tab
                    button = wait(driver, 2).until(EC.presence_of_element_located((By.XPATH, "//div[@id='photos']")))
                    dummy_driver = get_url('https://www.google.com', dummy_driver)
                    driver.execute_script("arguments[0].click();", button)
                    time.sleep(1)
                    break
                except Exception as err:
                    driver.refresh()
                    time.sleep(1)

            hotel['Bedroom & bathroom Images'] = ''
            hotel['Exterior, Interior & Amenities Images'] = ''
            hotel['Food & drink Images'] = ''
            # selecting the images posted by the owner
            owner = False
            try:  
                dummy_driver = get_url('https://www.google.com', dummy_driver)
                buttons = wait(driver, 2).until(EC.presence_of_all_elements_located((By.XPATH, "//button[@class='VfPpkd-LgbsSe VfPpkd-LgbsSe-OWXEXe-INsAgc VfPpkd-LgbsSe-OWXEXe-dgl2Hf Rj2Mlf OLiIxf PDpWxe P62QJc LQeN7 rRDaU xnu6rd QGRmIf']")))
                for button in buttons:
                    if 'By owner' in button.get_attribute('textContent'):
                        pressed = button.get_attribute('aria-pressed')
                        if pressed == 'false':
                            dummy_driver = get_url('https://www.google.com', dummy_driver)
                            driver.execute_script("arguments[0].click();", button)
                        time.sleep(2)
                        owner = True
                        break
            except Exception as err:
                pass

            # for rentals
            try:
                if prop_type == 'Holiday rentals':
                    # searching Google for the rental website
                    search_link = 'https://www.google.com/maps/search/' + hotel['Name'] + ', ' + dest[0].split(',')[0] + '?hl=en'
                    driver.get(search_link)
                    time.sleep(2)

                    # location details
                    add, website, plus  = '', '', ''
                    try:
                        try:
                            divs = wait(driver, 2).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div[class*='RcCsl fVHpi w4vB1d NOE9ve M0S7ae AG25L']")))
                        except Exception as err:
                            search_link = 'https://www.google.com/maps/search/' + hotel['Name'] + ', ' + dest[0].split(',')[-1] + '?hl=en'
                            driver.get(search_link)
                            time.sleep(2)
                            divs = wait(driver, 2).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div[class*='RcCsl fVHpi w4vB1d NOE9ve M0S7ae AG25L']")))

                        for div in divs:
                            try:
                                button = wait(div, 2).until(EC.presence_of_element_located((By.TAG_NAME, "button")))
                                try:
                                    a = wait(div, 2).until(EC.presence_of_element_located((By.TAG_NAME, "a")))
                                    if website == '' or website == None:
                                        website = a.get_attribute('href')
                                except Exception as err:
                                    pass

                                if button is NoneType or button is None: 
                                    continue

                                label = button.get_attribute('aria-label')
                                if label is NoneType or label is None:
                                    continue
                                if 'Plus code:' in label and plus == '':
                                    plus = label.split(':')[-1].strip()
                                elif 'Address:' in label and add == '':
                                    add = label.split(':')[-1].strip()
                                elif 'Phone:' in label and tel == '':
                                    tel = label.split(':')[-1].strip()
                            except Exception as err:
                                pass
                        if plus == '':
                            divs = wait(driver, 2).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div[class='Io6YTe fontBodyMedium']")))
                            for div in divs:
                                text = div.get_attribute('textContent')
                                if '+' in text and text[0] != '+' and 'LGBTQ' not in text:
                                    plus = text
                                    break
                    except Exception as err:
                        pass

                    if hotel['Address'] == '' and add != '':
                        hotel['Address'] = add
                        if hotel['Address'] != '':
                            output_msg(f"Address: {hotel['Address']}", 0)
                    if hotel['Website'] == '' and website != '':
                        hotel['Website'] = website
                        site = True
                    hotel['Google Maps Location'] = plus
                  
                    # check if site not in the exclusion list
                    for elem in exclusion:
                        if elem.lower() in hotel['Website'].lower():
                            site = False
                            output_msg(f"The website for '{hotel['Name']}' is found in the exclusion list, skipping images scraping ...", 0)

                    # if site is found and valid
                    if site:
                        driver.get(hotel['Website'])
                        time.sleep(2)
                        rental_links = ''
                        imgs = wait(driver, 2).until(EC.presence_of_all_elements_located((By.TAG_NAME, "img")))    
                        for img in imgs:
                            try:
                                url = img.get_attribute('src')
                                # ignoring gif images
                                if '.gif' in url or 'logo' in url.lower(): continue
                                # ignoring images with small dimensions
                                try:
                                    height = int(img.get_attribute('height'))
                                    if height < 400: continue
                                except Exception as err:
                                    pass
                                if 'https://' not in url and 'http://' not in url: continue
                                rental_links += url + ','
                            except Exception as err:
                                continue
                        if len(rental_links) > 0:
                            rental_links = rental_links[:-1]
                        hotel['Exterior, Interior & Amenities Images'] = rental_links 
            except Exception as err:
                pass

            # for hotels
            if owner and prop_type == 'Hotels':
                
                for _ in range(5):
                    try:
                        links1, links2, links3, success = get_hotel_images(driver, dummy_driver, prop_type)
                        if not success: 
                            driver.refresh()
                            time.sleep(2)
                            continue
                        else:
                            break
                    except Exception as err:
                        driver.refresh()
                        time.sleep(2)

                hotel['Bedroom & bathroom Images'] = links1
                hotel['Exterior, Interior & Amenities Images'] = links2
                hotel['Food & drink Images'] = links3

            if hotel['Location Rating'] == '' and prop_type == 'Hotels':
                hotel.clear()
                continue
            # adding the hotel info to the df
            df = df.append(hotel.copy(), ignore_index=True)
            scraped = df['Name'].values.tolist()
            # clearing the hotel dict
            hotel.clear()
            ihotel += 1
            if df.shape[0] == limit: break
            #if np.mod(ihotel, 20) == 0:
            #    try:
            #        driver.quit()
            #        driver = initialize_bot(driver_path)
            #    except Exception as err:
            #        pass

        except Exception as err:
            output_msg(f"Warning: Failed to scrape {prop_type[:-1]}: '{hotel['Name']}' due to the below error, skipping ...", 0)
            output_msg(err, 0)
            ihotel += 1
            continue

    elapsed = round((time.time() - start)/60, 2)
    hours = round(elapsed/60, 2)
    output_msg(f'{dest_name} is scraped in: {elapsed} mins ({hours} hours)', 1)
    driver.quit()
    return df, dest

def get_hotel_images(driver, dummy_driver, prop_type):

    links1, links2, links3 = '', '', ''
    # clicking on "By Owner" button
    try:  
        dummy_driver = get_url('https://www.google.com', dummy_driver)
        buttons = wait(driver, 2).until(EC.presence_of_all_elements_located((By.XPATH, "//button[@class='VfPpkd-LgbsSe VfPpkd-LgbsSe-OWXEXe-INsAgc VfPpkd-LgbsSe-OWXEXe-dgl2Hf Rj2Mlf OLiIxf PDpWxe P62QJc LQeN7 rRDaU xnu6rd QGRmIf']")))
        for button in buttons:
            if 'By owner' in button.get_attribute('textContent'):
                pressed = button.get_attribute('aria-pressed')
                if pressed == 'false':
                    dummy_driver = get_url('https://www.google.com', dummy_driver)
                    driver.execute_script("arguments[0].click();", button)
                time.sleep(3)
                owner = True
                break
    except Exception as err:
        pass

    dummy_driver = get_url('https://www.google.com', dummy_driver)
    htmlelement= wait(driver, 2).until(EC.presence_of_element_located((By.TAG_NAME, "html")))
    htmlelement.send_keys(Keys.END)
    time.sleep(2)
    sections = wait(driver, 2).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "section[class='NgCL1e']")))
    nsec = len(sections)
    try:
        for m in range(nsec):
            msg = True                    
            #sec = wait(driver, 2).until(EC.presence_of_all_elements_located((By.XPATH, "//section[@class='NgCL1e']")))[m]
            sec = sections[m]
            time.sleep(1)
            header = sec.get_attribute('textContent')
            if 'Summary' in header or 'At a glance' in header: continue      
            #showing all images
            try:
                button = wait(sec, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[jsname='oZzHLe']")))
                dummy_driver = get_url('https://www.google.com', dummy_driver)
                driver.execute_script("arguments[0].click();", button)
                time.sleep(1)
            except Exception as err:
                pass
            name = sec.get_attribute('textContent').split('(')[0].strip()
            imgs = wait(sec, 2).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div[class='M3UVH']")))
            time.sleep(1)
            for l, elem in enumerate(imgs):
                img_link = wait(elem, 2).until(EC.visibility_of_element_located((By.TAG_NAME, "img"))).get_attribute('src')
                if 'https://' not in img_link and 'http://' not in img_link: continue
                ind = img_link.index('=')
                img_link = img_link[:ind] + '=w1341-h813'
                if msg:
                    output_msg(f"Scraping {prop_type[:-1]} {name} images ...", 0)
                    msg = False
                if 'Bedroom' in name or 'Bathroom' in name:
                    links1 += img_link + ','
                elif 'Exterior' in name or 'Interior' in name or 'Amenities' in name:
                    links2 += img_link + ','
                else:
                    links3 += img_link + ','
    except Exception as err:
        return links1, links2, links3, False
                
    if len(links1) > 0:
        links1 = links1[:-1]            
    if len(links2) > 0:
        links2 = links2[:-1]            
    if len(links3) > 0:
        links3 = links3[:-1]

    return links1, links2, links3, True

def is_unique(s):
    # checking if the series has only one value
    a = s.to_numpy() # s.values (pandas<0.24)
    return (a[0] == a).all()

def get_google_loc(df, csv_folder, dest, prop_type, url, k):

    global dummy_driver, driver_path
    # processed destination
    if len(dest[1]) > 0:
        return df
    # output path for the csv file
    dest_name = dest[0].title().replace(',', '_').replace(' ', '_')
    path = os.getcwd() + '\\csv_files'
    output_msg(f'Processing the csv file for the {prop_type}', 1)
    #df = df.drop_duplicates(subset=['Name'])
    df = df.replace('', np.nan)
    df = df.dropna(axis=1, how='all')
    df = df.reset_index(drop = True)
    # reordering the csv file columns
    first_cols = ['Location',	'Name',	'Google Travel Link',	'Address',	'Phone Number',	'Website',	'Google Maps Location',	'Short Description', 'Neighbourhood', 'Location Description', 'Star Rating',	'Location Rating', 'Guest Rating', 'Rating Word', 'Number of Reviews', '5 star Guest rating Percentage',	'4 star Guest rating Percentage',	'3 star Guest rating Percentage',	'2 star Guest rating Percentage',	'1 star Guest rating Percentage', 'About',	'Check in Time',	'Check Out Time', 'Bedroom & bathroom Images',	'Exterior, Interior & Amenities Images',	'Food & drink Images']

    ordered_cols, tmp_cols = [], []
    cols = df.columns

    # dropping the columns with the empty strings
    for col in cols[2:]:
        if is_unique(df[col]):
            df = df.drop(col, axis=1)

    cols = df.columns
    for col in first_cols:
        if col in cols:
            ordered_cols.append(col)

    for col in cols:
        if 'Price' in col:
            ordered_cols.append(col)

    for col in cols:
        if col not in ordered_cols:
            tmp_cols.append(col)

    #tmp_cols.sort()
    ordered_cols += tmp_cols
    df = df[ordered_cols]

    output_msg(f'Scraping the Google Locations for {prop_type} in {dest[0].title()}', 1)
    inds = df.index
    n = df.shape[0]
    loc = df.loc[inds[0], 'Location']
    driver = initialize_bot(driver_path)
    for ind in inds:
        if 'Google Maps Location' in cols:
            if df.loc[ind, 'Google Maps Location'] != '':
                maps_loc = df.loc[ind, 'Google Maps Location']
                output_msg(f"{prop_type[:-1]} {ind+1}/{n} Google Maps Location: {maps_loc}", 0)
                continue
        name = df.loc[ind, 'Name'] 
        maps_loc = ' '
        # getting google maps location for the hotel
        try:
            driver = get_url('https://www.google.com/maps/?hl=en', driver)
            #time.sleep(1)
            search = wait(driver, 2).until(EC.presence_of_element_located((By.XPATH, "//input[@id='searchboxinput']")))
            search.send_keys(name + ', ' + loc)
            dummy_driver = get_url('https://www.google.com', dummy_driver)
            search.send_keys(Keys.ENTER)
            time.sleep(1)
            # for multiple search results
            try:
                button = wait(driver, 2).until(EC.presence_of_element_located((By.XPATH, "//a[@class='hfpxzc']")))
                dummy_driver = get_url('https://www.google.com', dummy_driver)
                driver.execute_script("arguments[0].click();", button)
                time.sleep(1)
            except Exception as err:
                pass
            div = wait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[aria-label*='Information for']")))
            buttons = wait(div, 2).until(EC.presence_of_all_elements_located((By.TAG_NAME, "button")))
            add = False
            for button in buttons:            
                if button is NoneType or button is None:
                    continue
                label = button.get_attribute('aria-label')
                if label is NoneType or label is None:
                    continue
                if 'Plus code' in label:
                    maps_loc = label.split(':')[-1].strip()
                    break
                elif 'Address:' in label and not add:
                    maps_loc = label.split(':')[-1].strip()
                    add = True
            df.loc[ind, 'Google Maps Location'] = maps_loc
        except Exception as err:
            try:
                label = wait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[class='MngOvd fontBodyMedium zWArOe']"))).get_attribute('textContent').strip()
                df.loc[ind, 'Google Maps Location'] = label
                maps_loc = label
            except Exception as err:
                df.loc[ind, 'Google Maps Location'] = loc
                maps_loc = loc

        output_msg(f"{prop_type[:-1]} {ind+1}/{n} Google Maps Location: {maps_loc}", 0) 

    if not os.path.isdir(path):
        os.makedirs(path)
    try:
        # reording the hotels by the number of reviews
        df['Number of Reviews'] = df['Number of Reviews'].astype(int)
        df = df.sort_values('Number of Reviews', ascending=False)
    except Exception as err:
        output_msg(f"Warning: Failed to sort the {prop_type} by the number of reviews ...", 1)
    output_msg(f"Exporting {prop_type} info to a local csv file ...", 1)
    # outputting local copy of the csv file
    df.to_csv(path + f"\\{dest_name}_{prop_type}.csv", encoding='UTF-8', index=False)

    # uploading the csv file
    uploaded = False
    for _ in range(3):
        try:
            csv_id = upload_csv(path + f"\\{dest_name}_{prop_type}.csv", csv_folder)
            uploaded = True
            break
        except Exception as err:
            error = str(err)
            output_msg(f'Error in uploading the csv file: {error}', 1)
            write_status(k+2, f'Error in uploading the csv file: {error}', url)
            time.sleep(60)

    if not uploaded:
        return pd.DataFrame()

    # moving the csv file to the right location on the drive
    moved = False
    for _ in range(3):
        try:
            dummy_driver = get_url('https://www.google.com', dummy_driver)
            location = df['Location'].unique().tolist()[0]
            link, folder_stamp = move_doc(csv_id, location, csv_folder, '')
            output_msg('The csv file is uploaded successfully, sharing link:', 1)
            output_msg(f'{link}', 0)
            moved = True
            break
        except Exception as err:
            err = str(err)
            output_msg(f'Error in moving the csv file: {err}', 1)
            write_status(k+2, f'Error in moving the csv file: {err}', url)
            time.sleep(60)

    driver.quit()
    if not moved:
        return pd.DataFrame()

    return df, folder_stamp

def upload_csv(path, csv_folder):

    global dummy_driver
    dummy_driver = get_url('https://www.google.com', dummy_driver)
    credentials = get_Google_API_creds()
    try:
        drive_service = build('drive', 'v3', credentials=credentials, cache_discovery=False, static_discovery=False)
    except Exception as err:
      DISCOVERY_SERVICE_URL = 'https://www.googleapis.com/drive/v3/'
      drive_service = build('drive', 'v3', credentials=credentials, discoveryServiceUrl=DISCOVERY_SERVICE_URL, cache_discovery=False, static_discovery=False)

    output_msg('Uploading the csv file ....', 1)

    name = path.split('\\')[-1]
    file_metadata = {'name': f"{name}", 'parents': [csv_folder]}
    dummy_driver = get_url('https://www.google.com', dummy_driver)
    media = MediaFileUpload(path, mimetype='text/csv')
    csv = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    csv_id = csv.get('id')

    return csv_id

def create_hotel_doc(df, dest, doc_folder, limit, curr, date1, date2, date3, date4, order, prices, website, prop_type, folder_stamp):

    global dummy_driver
    dummy_driver = get_url('https://www.google.com', dummy_driver)

    df = df.fillna('')
    symbol = CurrencySymbols.get_symbol(curr)
    nhotels = df.shape[0]

    # if df has only one hotel, no need to create a doc for it
    #if nhotels < 2:
    #    return ''

    # creating google doc
    if nhotels > limit:
        nhotels = limit

    output_msg(f'Creating Google doc for destination {dest[0].title()} ....', 1)
    try:
        dummy_driver = get_url('https://www.google.com', dummy_driver)
        doc_id, end_ind = create_google_doc(dest[0].title(), dest[2], doc_folder, prop_type)
    except Exception as err :
        err = str(err)
        return f'Error in creating Google doc: {err}'

    # failed to add the intro text
    if isinstance(end_ind, str):
        return end_ind

    # add title to the doc
    title = f"\nThe {nhotels} Most Popular {prop_type} In {dest[0].title()}"
    try:
        dummy_driver = get_url('https://www.google.com', dummy_driver)
        end_ind = add_title_to_google_doc(title, doc_id, end_ind)
    except Exception as err :
        err = str(err)
        return f'Error in adding title to Google doc: {err}'

    if isinstance(end_ind, str):
        return end_ind
    
    for i in range(nhotels):
        if order == 'Ascending':
            row = df.iloc[nhotels - i - 1]
        else:
            row = df.iloc[i]
        hotel_id = nhotels - i
        output_msg(f'Adding {prop_type[:-1]} {i+1}/{nhotels} to Google Doc ....', 0)
        if nhotels > 14 and hotel_id == 10:
            title = f"\nOur Top picks"
            try:
                dummy_driver = get_url('https://www.google.com', dummy_driver)
                end_ind = add_title_to_google_doc(title, doc_id, end_ind)
            except Exception as err:
                err = str(err)
                return f'Error in adding title to Google doc: {err}'

        try:
            dummy_driver = get_url('https://www.google.com', dummy_driver)
            end_ind = export_hotel(doc_id, row, hotel_id, end_ind, symbol, date1, date2, date3, date4, prices, website, prop_type)
        except Exception as err:
            err = str(err)
            return f'Error in adding {prop_type[:-1]} data to Google doc: {err}'

        if isinstance(end_ind, str):
            return end_ind

    # add ending text
    try:
        dummy_driver = get_url('https://www.google.com', dummy_driver)
        end_ind = add_ending_text(doc_id, end_ind, dest)
    except Exception as err:
        err = str(err)
        return f'Error in adding the final text to Google doc: {err}'

    if isinstance(end_ind, str):
        return end_ind
    
    # removing trailing spaces (caused the last hotel to be removed)
    #try:
    #    remove_empty_lines_via_API(doc_id)
    #except Exception as err:
    #    pass

    # moving the Google doc to the right location on the drive
    location = df['Location'].unique().tolist()[0].strip()
    try:
        dummy_driver = get_url('https://www.google.com', dummy_driver)
        link, folder_stamp = move_doc(doc_id, location, doc_folder, folder_stamp)
        output_msg('The Google doc is exported successfully, sharing link:', 1)
        output_msg(f'{link}', 0)
    except Exception as err:
        err = str(err)
        return f'Error in moving Google doc: {err}'

    return ''

def add_ending_text(doc_id, end_ind, dest):

    global dummy_driver
    dummy_driver = get_url('https://www.google.com', dummy_driver)
    # no ending or newsletter inputs are provided
    if len(dest) < 4:
        return end_ind

    # add ending text
    #text = f'Thank you for checking out our list of the best hotels in {dest[0].title()}! We hope you found some places to stay. Happy travels!\n'
    text = '\n\n' + dest[3] + '\n\n'
    start_ind =  end_ind 
    end_ind = start_ind + len(text)
    success = False
    err_msg = ''
    for _ in range(10):
        try:
            dummy_driver = get_url('https://www.google.com', dummy_driver)
            add_text_via_API(doc_id, text, start_ind, end_ind, 'Martel', 13, False, False, 0, 0, 0)
            success = True
            break
        except Exception as err:
            start_ind -= 1
            err_msg = str(err)
    if not success:
            msg = 'Failure in exporting final text to Google doc, please add manually\n' + err_msg
            return msg

    # no news letter is provided
    if len(dest) < 5:
        return end_ind

    text = 'Join the Thailandos community\n\n'
    start_ind = end_ind
    end_ind = start_ind + len(text)
    success = False
    err_msg = ''
    for _ in range(10):
        try:
            dummy_driver = get_url('https://www.google.com', dummy_driver)
            add_heading_via_API(doc_id, text, start_ind, end_ind, 'Martel', 13, True, False, 0, 0, 0, 'HEADING_3')
            success = True
            break
        except Exception as err:
            start_ind -= 1
            err_msg = str(err)
        if not success:
            msg = 'Failure in exporting final text to Google doc, please add manually\n' + err_msg
            return msg

    #text = 'At Thailandos, our team works tirelessly to bring you the most comprehensive itineraries and reviews to help you plan your next trip. Don’t miss out, join our mailing list and our reviews delivered directly to your mailbox!\n'
    text = dest[4] + '\n\n'
    start_ind = end_ind 
    end_ind = start_ind + len(text)
    success = False
    err_msg = ''
    for _ in range(10):
        try:
            dummy_driver = get_url('https://www.google.com', dummy_driver)
            add_text_via_API(doc_id, text, start_ind, end_ind, 'Martel', 13, False, False, 0, 0, 0)
            success = True
            break
        except Exception as err:
            start_ind -= 1
            err_msg = str(err)
        if not success:
            msg = 'Failure in exporting final text to Google doc, please add manually\n' + err_msg
            return msg

    return end_ind

def output_msg(msg, newline):

    global stamp

    if newline == 1:
        logging.info('-'*75)
    logging.info(msg)
    timestamp = datetime.now().strftime("%Y-%m-%d %I:%M %p")
    with open(f'session_log_{stamp}.log', 'a', newline='', encoding='UTF-8') as f:
        if newline == 1:
            f.write('-'*75)
            f.write('\n')
        f.write(f'{timestamp} - {msg}\n')


def add_text_via_API(doc_id, text, start_ind, end_ind, font, size, bold, italic, blue, green, red):

    global dummy_driver
    dummy_driver = get_url('https://www.google.com', dummy_driver)

    credentials = get_Google_API_creds()
    try:
        service = build('docs', 'v1', credentials=credentials, cache_discovery=False, static_discovery=False)
    except Exception as err:
        DISCOVERY_SERVICE_URL = 'https://docs.googleapis.com'
        service = build('docs', 'v1', credentials=credentials, discoveryServiceUrl=DISCOVERY_SERVICE_URL, cache_discovery=False, static_discovery=False)

    requests = [{'insertText': {'location': {'index': start_ind},'text': text}}, {"updateParagraphStyle": {"range": {"startIndex": start_ind,"endIndex": end_ind},"paragraphStyle": {"alignment": "START"},"fields": "alignment"}}, {'updateTextStyle': {'range': {'startIndex': start_ind,'endIndex':end_ind},'textStyle': {'weightedFontFamily': {'fontFamily': font}, 'bold': bold, 'italic': italic,'fontSize': {'magnitude': size,'unit': 'PT'}, 'foregroundColor': {'color': {'rgbColor': {'blue': blue,'green': green,'red': red}}}},'fields':'foregroundColor, bold, italic, weightedFontFamily, fontSize'}}]
    dummy_driver = get_url('https://www.google.com', dummy_driver)
    service.documents().batchUpdate(documentId=doc_id, body={'requests': requests}).execute()

def read_paragraph_element(element):
    """Returns the text in the given ParagraphElement.

        Args:
            element: a ParagraphElement from a Google Doc.
    """
    text_run = element.get('textRun')
    if not text_run:
        return ''
    return text_run.get('content')

def read_structural_elements(elements):
    """Recurses through a list of Structural Elements to read a document's text where text may be
        in nested elements.

        Args:
            elements: a list of Structural Elements.
    """
    text = ''
    for value in elements:
        if 'paragraph' in value:
            elements = value.get('paragraph').get('elements')
            for elem in elements:
                text += read_paragraph_element(elem)
        elif 'table' in value:
            # The text in table cells are in nested Structural Elements and tables may be
            # nested.
            table = value.get('table')
            for row in table.get('tableRows'):
                cells = row.get('tableCells')
                for cell in cells:
                    text += read_structural_elements(cell.get('content'))
        elif 'tableOfContents' in value:
            # The text in the TOC is also in a Structural Element.
            toc = value.get('tableOfContents')
            text += read_structural_elements(toc.get('content'))
    return text

def remove_empty_lines_via_API(doc_id):

    global dummy_driver
    dummy_driver = get_url('https://www.google.com', dummy_driver)

    credentials = get_Google_API_creds()
    try:
        service = build('docs', 'v1', credentials=credentials, cache_discovery=False, static_discovery=False)
    except Exception as err:
        DISCOVERY_SERVICE_URL = 'https://docs.googleapis.com'
        service = build('docs', 'v1', credentials=credentials, discoveryServiceUrl=DISCOVERY_SERVICE_URL, cache_discovery=False, static_discovery=False)

    doc = service.documents().get(documentId=doc_id).execute()
    contents = doc.get('body').get('content')
    text = read_structural_elements(contents)
    end_ind = contents[-1]["endIndex"] - 1
    x = re.search("\s+$", text)
    start_ind = x.start()       

    if end_ind - start_ind > 150:
        requests = [{'deleteContentRange': {"range": {"startIndex": start_ind,"endIndex": end_ind}}}]
        dummy_driver = get_url('https://www.google.com', dummy_driver)
        service.documents().batchUpdate(documentId=doc_id, body={'requests': requests}).execute()


    
def add_heading_via_API(doc_id, text, start_ind, end_ind, font, size, bold, italic, blue, green, red, heading):

    global dummy_driver
    dummy_driver = get_url('https://www.google.com', dummy_driver)
    credentials = get_Google_API_creds()
    try:
        service = build('docs', 'v1', credentials=credentials, cache_discovery=False, static_discovery=False)
    except Exception as err:
        DISCOVERY_SERVICE_URL = 'https://docs.googleapis.com'
        service = build('docs', 'v1', credentials=credentials, discoveryServiceUrl=DISCOVERY_SERVICE_URL, cache_discovery=False, static_discovery=False)
    requests = [{'insertText': {'location': {'index': start_ind},'text': text}}, {"updateParagraphStyle": {"range": {"startIndex": start_ind,"endIndex": end_ind},"paragraphStyle": {"namedStyleType": heading, "alignment": "START"},"fields": "alignment, namedStyleType"}}, {'updateTextStyle': {'range': {'startIndex': start_ind,'endIndex':end_ind},'textStyle': {'weightedFontFamily': {'fontFamily': font}, 'bold': bold, 'italic': italic,'fontSize': {'magnitude': size,'unit': 'PT'}, 'foregroundColor': {'color': {'rgbColor': {'blue': blue,'green': green,'red': red}}}},'fields':'foregroundColor, bold, italic, weightedFontFamily, fontSize'}}]
    dummy_driver = get_url('https://www.google.com', dummy_driver)
    service.documents().batchUpdate(documentId=doc_id, body={'requests': requests}).execute()
    
def add_hyperlink_via_API(doc_id, text, link, start_ind, end_ind, style_start_ind, style_end_ind, font, size, bold, italic, blue, green, red):
    global dummy_driver
    dummy_driver = get_url('https://www.google.com', dummy_driver)
    credentials = get_Google_API_creds()
    try:
        service = build('docs', 'v1', credentials=credentials, cache_discovery=False, static_discovery=False)
    except Exception as err:
        DISCOVERY_SERVICE_URL = 'https://docs.googleapis.com'
        service = build('docs', 'v1', credentials=credentials, discoveryServiceUrl=DISCOVERY_SERVICE_URL, cache_discovery=False, static_discovery=False)

    requests = [{'insertText': {'location': {'index': start_ind},'text': text}}, {'updateTextStyle': {'range': {'startIndex': start_ind,'endIndex':end_ind},'textStyle': {'weightedFontFamily': {'fontFamily': font}, 'bold': bold, 'italic': italic,'fontSize': {'magnitude': size,'unit': 'PT'}, 'foregroundColor': {'color': {'rgbColor': {'blue': blue,'green': green,'red': red}}}},'fields':'foregroundColor, bold, italic, weightedFontFamily, fontSize'}}, {"updateParagraphStyle": {"range": {"startIndex": start_ind,"endIndex": end_ind},"paragraphStyle": {"alignment": "START"}, "fields": "alignment"}}, {'updateTextStyle': {'range': {'startIndex': style_start_ind, 'endIndex': style_end_ind} ,'textStyle': {'link':{'url': link}},'fields': 'link'}}]
    dummy_driver = get_url('https://www.google.com', dummy_driver)
    service.documents().batchUpdate(documentId=doc_id, body={'requests': requests}).execute()

def create_google_doc(dest, text, folder_id, prop_type):
    
    global dummy_driver
    dummy_driver = get_url('https://www.google.com', dummy_driver)
    credentials = get_Google_API_creds()
    #output_msg(f'Creating Google doc for {dest} ...', 1)

    try:
        drive_service = build('drive', 'v3', credentials=credentials, cache_discovery=False, static_discovery=False)
    except Exception as err:
        DISCOVERY_SERVICE_URL = 'https://www.googleapis.com/drive/v3/'
        drive_service = build('drive', 'v3', credentials=credentials, discoveryServiceUrl=DISCOVERY_SERVICE_URL, cache_discovery=False, static_discovery=False)

    title = f"Best {prop_type} In {dest}"
    body = {'name': title, 'mimeType': 'application/vnd.google-apps.document', 'parents': [folder_id]}

    try:
        # checking destinations files in the input folder
        attr_files = {}
        dummy_driver = get_url('https://www.google.com', dummy_driver)
        query = f'mimeType != "application/vnd.google-apps.folder" and trashed = false and name = "{title}" and "{folder_id}" in parents'
        response = drive_service.files().list(q = query).execute()
        for attr_file in response.get('files', []):
            attr_files[attr_file.get('name')] = attr_file.get('id')

        # if there is an old destination file with the same name then remove it to be replaced with the updated one
        dummy_driver = get_url('https://www.google.com', dummy_driver)
        if title in attr_files.keys():
            attr_file_id = attr_files[title]
            drive_service.files().delete(fileId=attr_file_id).execute()
    except Exception as err:
        pass

    #creating the new document
    dummy_driver = get_url('https://www.google.com', dummy_driver)
    doc = drive_service.files().create(body=body, fields='id').execute()
    doc_id = doc.get('id')

    # adding contents of the intro
    start_ind = 1
    end_ind = 1
    if len(text) > 0 and text != 'nan':
        text = text + '\n\n'
        end_ind =  start_ind + len(text)       
        success = False
        err_msg = ''
        #for _ in range(10):
        try:
            dummy_driver = get_url('https://www.google.com', dummy_driver)
            add_text_via_API(doc_id, text, start_ind, end_ind, 'Martel', 13, False, False, 0, 0, 0)
            success = True
            #break
        except Exception as err:
            end_ind =  1
            err_msg = str(err)
        #if not success:
        #    msg = f'Failed to export the intro text to Google doc for destination: {dest}\n' + err_msg
        #    output_msg(msg, 1)

        #    return '-1', msg
                
        # new lines for the title
        text = '\n\n'
        start_ind = end_ind 
        end_ind =  start_ind + len(text) 
        #for _ in range(10):
        try:
            dummy_driver = get_url('https://www.google.com', dummy_driver)
            add_text_via_API(doc_id, text, start_ind, end_ind, 'Martel', 20, False, False, 0, 0, 0)
            #break
        except Exception as err:
            end_ind -= len(text) 

    return doc_id, end_ind

def add_title_to_google_doc(text, doc_id, start_ind):
    global dummy_driver
    dummy_driver = get_url('https://www.google.com', dummy_driver)
    # title 
    text = text + '\n'
    start_ind = start_ind
    end_ind =  start_ind + len(text) 
    success = False
    err_msg = ''
    #for _ in range(10):
    try:
        dummy_driver = get_url('https://www.google.com', dummy_driver)
        add_heading_via_API(doc_id, text, start_ind, end_ind, 'Reem Kufi', 30, True, False, 0.51, 0.23, 0.25, 'HEADING_2')
        success = True
        #break
    except Exception as err:
        end_ind -= len(text) 
        err_msg = str(err)
    #if not success:
    #    msg = 'Failed to export the title text to Google doc\n' + err_msg
    #    return msg

    return end_ind


def export_hotel(doc_id, attr, attr_id, start_id, symbol, date1, date2, date3, date4, prices, website, prop_type):

    global dummy_driver
    dummy_driver = get_url('https://www.google.com', dummy_driver)    
    credentials = get_Google_API_creds()
    try:
        service = build('docs', 'v1', credentials=credentials, cache_discovery=False, static_discovery=False)
    except Exception as err:
        DISCOVERY_SERVICE_URL = 'https://docs.googleapis.com'
        service = build('docs', 'v1', credentials=credentials, discoveryServiceUrl=DISCOVERY_SERVICE_URL, cache_discovery=False, static_discovery=False)

    # hotel name
    text =  str(attr_id) + '. ' + str(attr['Name']) + '\n'
    start_ind = start_id 
    end_ind =  start_ind + len(text)
    success = False
    err_msg = ''
    #for _ in range(10):         
    try:
        dummy_driver = get_url('https://www.google.com', dummy_driver)
        add_heading_via_API(doc_id, text, start_ind, end_ind, 'Reem Kufi', 30, True, False, 0.51, 0.23, 0.25, 'HEADING_3')
        success = True
        #break
    except Exception as err:
        end_ind -= len(text)
        #err_msg = str(err)
    #if not success:
    #    msg = f"Failed to export the {prop_type[:-1]} name '{attr['Name']}' to Google doc\n" + err_msg
    #    return msg
    
    try:
        # Hotel class 
        text = str(attr['Star Rating']) + '\n'
        if len(attr['Star Rating']) > 0:
            start_ind = end_ind 
            end_ind =  start_ind + len(text)   
            success = False
            err_msg = ''
            #for _ in range(10):
            try:
                dummy_driver = get_url('https://www.google.com', dummy_driver)
                add_text_via_API(doc_id, text, start_ind, end_ind, 'Urbanist', 12, True, True, 0.51, 0.23, 0.25) 
                success = True
                #break
            except Exception as err:
                end_ind -= len(text)
                err_msg = str(err)
            #if not success:
            #    msg = f"Failed to export the rating for {prop_type[:-1]}: '{attr['Name']}' to Google doc\n" + err_msg 
            #    return msg
    except Exception as err:
        pass
     
    try:
        # Hotel rating 
        text = str(attr['Guest Rating']/2) + ' '
        if len(str(attr['Guest Rating'])) > 0:
            start_ind = end_ind 
            end_ind =  start_ind + len(text)   
            success = False
            err_msg = ''
            #for _ in range(10):
            try:
                dummy_driver = get_url('https://www.google.com', dummy_driver)
                add_text_via_API(doc_id, text, start_ind, end_ind, 'Urbanist', 12, True, True, 0.51, 0.23, 0.25) 
                success = True
                #break
            except Exception as err:
                end_ind -= len(text)
                err_msg = str(err)

            #if not success:
            #    msg = f"Failed to export the rating for {prop_type[:-1]}: '{attr['Name']}' to Google doc\n" + err_msg
            #    return msg    
    except Exception as err:
        pass
        
    try:
        # Hotel rating word
        text = str(attr['Rating Word']) + ' '
        if len(attr['Rating Word']) > 0:
            start_ind = end_ind 
            end_ind =  start_ind + len(text)   
            success = False
            err_msg = ''
            #for _ in range(10):
            try:
                dummy_driver = get_url('https://www.google.com', dummy_driver)
                add_text_via_API(doc_id, text, start_ind, end_ind, 'Urbanist', 12, True, True, 0.51, 0.23, 0.25) 
                success = True
                #break
            except Exception as err:
                end_ind -= len(text)
                err_msg = str(err)
            #if not success:
            #    msg = f"Failed to export the rating for {prop_type[:-1]}: '{attr['Name']}' to Google doc\n" + err_msg
            #    return msg
    except Exception as err:
        pass

    try:
        # attraction rating stars
        if len(str(attr['Guest Rating'])) > 0: 
            text = ''
            full_stars = int(attr['Guest Rating']/2)
            frac = int(str(attr['Guest Rating']/2).split('.')[-1])
            for _ in range(full_stars):
                text +=  '★'
            if frac >= 5 and full_stars < 5:
                text +=  '★'
            elif frac < 5 and full_stars < 5:
                text +=  '☆'
            nstars = 5 - len(text)
            for _ in range(nstars):
                text +=  '☆'
            if len(text) > 0:
                start_ind = end_ind 
                end_ind =  start_ind + len(text)  
                success = False
                err_msg = ''
                #for _ in range(10):
                try:
                    dummy_driver = get_url('https://www.google.com', dummy_driver)
                    add_text_via_API(doc_id, text, start_ind, end_ind, 'Urbanist', 11, False, True, 0, 0.7, 1)
                    success = True
                    #break
                except Exception as err:
                    end_ind -= len(text)
                    err_msg = str(err)
                #if not success: 
                #    msg = f"Failed to export the rating stars for {prop_type[:-1]} '{attr['Name']}' to Google doc\n" + err_msg
                #    return msg
    except Exception as err:
        pass
    
    try:
        # attraction number of reviews 
        nrev = int(attr['Number of Reviews'])
        if  nrev > 1:
            text = ' (' + format(nrev, ',d') + ' reviews)'
        else:
            text = ' (' + format(nrev, ',d') + ' review)'
        if  nrev > 0:
            text = text + '\n'
            start_ind = end_ind     
            end_ind =  start_ind + len(text)       
            success = False
            err_msg = ''
            #for _ in range(10):
            try:
                dummy_driver = get_url('https://www.google.com', dummy_driver)
                add_text_via_API(doc_id, text, start_ind, end_ind, 'Urbanist', 11, False, True, 0.32, 0.32, 0.32)
                success = True
                #break
            except Exception as err:
                end_ind -= len(text)
                err_msg = str(err)
            #if not success:
            #    msg = f"Failed to export the reviews for {prop_type[:-1]}: '{attr['Name']}' to Google doc\n" + err_msg
            #    return  msg
    except Exception as err:
        pass

    try:
        # Hotel images 
        img_found = False
        keys = attr.keys()
        if 'Bedroom & bathroom Images' in keys or 'Exterior, Interior & Amenities Images' in keys: 
            prev_img = ''
            for j in range(3):
                if (j == 0 or j == 2) and 'Exterior, Interior & Amenities Images' not in keys: continue
                if j == 1 and 'Bedroom & bathroom Images' not in keys: continue
                img = ''
                if j == 0 and len(attr['Exterior, Interior & Amenities Images']) > 0:
                    img = attr['Exterior, Interior & Amenities Images'].split(',')[0]
                elif j == 1 and len(attr['Bedroom & bathroom Images']) > 0:
                    img = attr['Bedroom & bathroom Images'].split(',')[0]
                elif j == 2 and len(attr['Exterior, Interior & Amenities Images']) > 1:
                    img = attr['Exterior, Interior & Amenities Images'].split(',')[-1]
                
                if img != '' and img != prev_img:
                    start_ind = end_ind 
                    end_ind = start_ind + 1
                    success = False
                    try:
                        dummy_driver = get_url('https://www.google.com', dummy_driver)
                        # adding image
                        requests = [{'insertInlineImage': {'location': {'index': start_ind},'uri':img,'objectSize': {'height':{'magnitude':400,'unit':'PT'},'width': {'magnitude': 400,'unit': 'PT'}}}}, {"updateParagraphStyle": {"range": {"startIndex": start_ind,"endIndex": end_ind},"paragraphStyle": {"alignment": "START"},"fields": "alignment"}}]
                        service.documents().batchUpdate(documentId=doc_id, body={'requests': requests}).execute()
                        prev_img = img
                        img_found = True
                    except Exception as err:
                        end_ind -= 1

    except Exception as err:
        pass

    try:
        if img_found:
            # attraction image credit
            text = "\nCredit: "
            start_ind = end_ind 
            end_ind =  start_ind + len(text) 
            success = False
            err_msg = ''
            #for _ in range(10):
            try:
                dummy_driver = get_url('https://www.google.com', dummy_driver)
                add_text_via_API(doc_id, text, start_ind, end_ind, 'Urbanist', 10, False, False, 0.51, 0.23, 0.25)
                success = True
                #break
            except Exception as err:
                end_ind -= len(text)
                err_msg = str(err)
            #if not success:
            #    msg = f"Failed to export the image credit for {prop_type[:-1]}: '{attr['Name']}' to Google doc\n" + err_msg             
            #    return msg
            
            # attraction image credit
            text = f"{attr['Name']}" + ' '
            start_ind = end_ind 
            end_ind =  start_ind + len(text) 
            success = False
            err_msg = ''
            #for _ in range(10):
            try:
                dummy_driver = get_url('https://www.google.com', dummy_driver)
                add_hyperlink_via_API(doc_id, text, prev_img, start_ind, end_ind, start_ind, end_ind, 'Urbanist', 10, False, False, 0.51, 0.23, 0.25)
                success = True
                #break
            except Exception as err:
                end_ind -= len(text)
                err_msg = str(err)
            #if not success:
            #    msg = f"Failed to export the image credit for {prop_type[:-1]}: '{attr['Name']}' to Google doc\n" + err_msg             
            #    return msg
    except Exception as err:
        pass

    try:
        address = False
        # Location text
        add_text = attr['Address']
        if isinstance(add_text, str) and len(add_text) > 0:
            address = True
            if img_found:
                text = '\n\nLocation: '
            else:
                text = '\nLocation: '
            start_ind = end_ind 
            end_ind =  start_ind + len(text) 
            success = False
            err_msg = ''
            #for _ in range(10):
            try:
                dummy_driver = get_url('https://www.google.com', dummy_driver)
                add_text_via_API(doc_id, text, start_ind, end_ind, 'Urbanist', 13, True, False, 0.51, 0.23, 0.25)
                success = True
                #break
            except Exception as err:
                end_ind -= len(text)
                err_msg = str(err)
            #if not success:
            #    msg = f"Failed to export the address for {prop_type[:-1]}: '{attr['Name']}' to Google doc\n" + err_msg        
            #    return msg
            
            # address text
            text = add_text
            start_ind = end_ind 
            end_ind =  start_ind + len(text) 
            success = False
            err_msg = ''
            #for _ in range(10):
            try:
                dummy_driver = get_url('https://www.google.com', dummy_driver)
                add_text_via_API(doc_id, text, start_ind, end_ind, 'Martel', 13, False, False, 0, 0, 0) 
                success = True
                #break
            except Exception as err:
                end_ind -= len(text)
                err_msg = str(err)
            #if not success:
            #    msg = f"Failed to export the address for {prop_type[:-1]}: '{attr['Name']}' to Google doc\n" + err_msg         
            #    return msg
    except Exception as err:
        pass

    try:
        phone = False
        text = attr['Phone Number']
        if isinstance(text, str) and len(text) > 0:
            phone = True
            # reformat if no address is available
            if address:
                text = '\nTel:  '
            else:
                text = '\n\nTel:  '
            start_ind = end_ind 
            end_ind =  start_ind + len(text) 
            success = False
            err_msg = ''
            #for _ in range(10):
            try:
                dummy_driver = get_url('https://www.google.com', dummy_driver)
                add_text_via_API(doc_id, text, start_ind, end_ind, 'Urbanist', 13, True, False, 0.51, 0.23, 0.25) 
                success = True
                #break
            except Exception as err:
                end_ind -= len(text)
                err_msg = str(err)
            #if not success:
            #    msg = f"Failed to export the phone number for {prop_type[:-1]}: '{attr['Name']}' to Google doc\n" + err_msg 
            #    return msg

            # phone text
            text = attr['Phone Number']
            start_ind = end_ind 
            end_ind =  start_ind + len(text)
            success = False
            err_msg = ''
            #for _ in range(10):
            try:
                dummy_driver = get_url('https://www.google.com', dummy_driver)
                add_text_via_API(doc_id, text, start_ind, end_ind, 'Martel', 13, False, False, 0, 0, 0)
                success = True
                #break
            except Exception as err:
                end_ind -= len(text)
                err_msg = str(err)
            #if not success:
            #    msg = f"Failed to export the phone number for {prop_type[:-1]}: '{attr['Name']}' to Google doc\n" + err_msg           
            #    return msg
 
    except Exception as err:
        pass

    try:
        if website:
            text = attr['Website']
            if isinstance(text, str) and len(text) > 0:
                # reformat if no address or phone are available
                if not address and not phone:
                    text = '\n\nWeb Address:   '
                else:
                    text = '\nWeb Address:   '
                start_ind = end_ind 
                end_ind =  start_ind + len(text) 
                success = False
                err_msg = ''
                #for _ in range(10):
                try:
                    dummy_driver = get_url('https://www.google.com', dummy_driver)
                    add_text_via_API(doc_id, text, start_ind, end_ind, 'Urbanist', 13, True, False, 0.51, 0.23, 0.25)
                    success = True
                    #break
                except Exception as err:
                    end_ind -= len(text)
                    err_msg = str(err)
                #if not success:
                #    msg = f"Failed to export the website for {prop_type[:-1]}: '{attr['Name']}' to Google doc\n" + err_msg
                #    return msg
              
                # website text
                text = attr['Website']
                start_ind = end_ind 
                end_ind =  start_ind + len(text) 
                success = False
                err_msg = ''
                #for _ in range(10):
                try:
                    dummy_driver = get_url('https://www.google.com', dummy_driver)
                    add_text_via_API(doc_id, text, start_ind, end_ind, 'Martel', 13, False, False, 0, 0, 0)
                    success = True
                    #break
                except Exception as err:
                    end_ind -= len(text)
                    err_msg = str(err)
                #if not success:
                #    msg = f"Failed to export the website for {prop_type[:-1]}: '{attr['Name']}' to Google doc\n" + err_msg 
                #    return msg
    except Exception as err:
        pass

    try:
        if prices:
            # hotel prices
            months = [date1, date2, date3, date4]
            for j in range(4):
                if months[j] == 'Disable': continue
                if len(str(attr[f'{months[j]} Min Price'])) == 0: continue
                text = f'\nAverage Pricing In {months[j]}:  '
                start_ind = end_ind 
                end_ind =  start_ind + len(text)
                success = False
                err_msg = ''
                #for _ in range(10):
                try:
                    dummy_driver = get_url('https://www.google.com', dummy_driver)
                    add_text_via_API(doc_id, text, start_ind, end_ind, 'Urbanist', 13, True, False, 0.51, 0.23, 0.25) 
                    success = True
                    #break
                except Exception as err:
                    end_ind -= len(text)
                    err_msg = str(err)
                #if not success:
                #    msg = f"Failed to export the pricing for {prop_type[:-1]}: '{attr['Name']}' to Google doc\n" + err_msg           
                #    return msg    
                avr_price = (attr[f'{months[j]} Min Price'] + attr[f'{months[j]} Max Price'])/2 
                text = f"{avr_price}{symbol}"
                start_ind = end_ind 
                end_ind =  start_ind + len(text)
                success = False
                err_msg = ''
                #for _ in range(10):
                try:
                    dummy_driver = get_url('https://www.google.com', dummy_driver)
                    add_text_via_API(doc_id, text, start_ind, end_ind, 'Martel', 13, False, False, 0, 0, 0)
                    success = True
                    #break
                except Exception as err:
                    end_ind -= len(text)
                    err_msg = str(err)
                #if not success:
                #    msg = f"Failed to export the pricing for {prop_type[:-1]}: '{attr['Name']}' to Google doc\n" + err_msg           
                #    return msg    
    except Exception as err:
        pass
    
    try:
        # new line for the next hotel
        text = '\n\n'
        start_ind = end_ind 
        end_ind =  start_ind + len(text) 
        #for _ in range(10):
        try:
            dummy_driver = get_url('https://www.google.com', dummy_driver)
            add_text_via_API(doc_id, text, start_ind, end_ind, 'Martel', 15, False, False, 0, 0, 0)
            success = True
            #break
        except Exception as err:
            end_ind -= len(text)
        #if not success:
        #    end_ind = end_ind - len(text)

    except Exception as err:
        pass

    return end_ind

def move_doc(doc_id, country, folder_id, folder_stamp):

    global dummy_driver
    dummy_driver = get_url('https://www.google.com', dummy_driver)
    credentials = get_Google_API_creds()
    try:
        drive_service = build('drive', 'v3', credentials=credentials, cache_discovery=False, static_discovery=False)
    except Exception as err:
        DISCOVERY_SERVICE_URL = 'https://www.googleapis.com/drive/v3/'
        drive_service = build('drive', 'v3', credentials=credentials, discoveryServiceUrl=DISCOVERY_SERVICE_URL, cache_discovery=False, static_discovery=False)

    ## getting existing folders names in the google drive under the input folder
    date_folders = {}
    response = drive_service.files().list(q = f'mimeType = "application/vnd.google-apps.folder" and trashed = false and "{folder_id}" in parents').execute()
    dummy_driver = get_url('https://www.google.com', dummy_driver)
    for folder in response.get('files', []):
        date_folders[folder.get('name')] = folder.get('id')


    # creating date folder inside the user input folder if not created yet
    if folder_stamp == '':
        folder_stamp = datetime.now().strftime("%d-%b-%y")

    folder_name = folder_stamp
    if folder_name not in date_folders.keys():
        try:
            file_metadata = {'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [folder_id]}
            dummy_driver = get_url('https://www.google.com', dummy_driver)
            folder = drive_service.files().create(body=file_metadata,fields='id').execute()
            date_folder_id = folder.get('id')
        except Exception as err:
            output_msg('The following error occurred while moving the document to the user specified folder on Google drive', 0)
            output_msg(err, 0)
    else:
        # folder already created
        date_folder_id = date_folders[folder_name]

    # creating a state folder for US destinations
    #if dest.find(',') > 0:
    #    country = dest.split(',')[-1].strip()

    # creating country folder inside the date one
    dest_folders = {}
    query = f'mimeType = "application/vnd.google-apps.folder" and trashed = false and "{date_folder_id}" in parents'
    dummy_driver = get_url('https://www.google.com', dummy_driver)
    response = drive_service.files().list(q = query).execute()
    for folder in response.get('files', []):
        dest_folders[folder.get('name')] = folder.get('id')
    if country not in dest_folders.keys():
        file_metadata = {'name': country, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [date_folder_id]}
        dummy_driver = get_url('https://www.google.com', dummy_driver)
        folder = drive_service.files().create(body=file_metadata,fields='id').execute()
        dest_folder_id = folder.get('id')
        #print (f'Folder ID: {folder_id}') 
    else:
        # folder already created
        dest_folder_id = dest_folders[country]

    # checking destinations files in the country folder
    attr_files = {}
    query = f'mimeType != "application/vnd.google-apps.folder" and trashed = false and "{dest_folder_id}" in parents'
    dummy_driver = get_url('https://www.google.com', dummy_driver)
    response = drive_service.files().list(q = query).execute()
    for attr_file in response.get('files', []):
        attr_files[attr_file.get('name')] = attr_file.get('id')

    # if there is an old destination file with the same name then remove it to be replaced with the updated one
    dummy_driver = get_url('https://www.google.com', dummy_driver)
    doc = drive_service.files().get(fileId=doc_id).execute()
    # getting the name from the doc dictionary
    title = doc.get('name', 'None')
    if title in attr_files.keys():
        attr_file_id = attr_files[title]
        dummy_driver = get_url('https://www.google.com', dummy_driver)
        drive_service.files().delete(fileId=attr_file_id).execute()

    # moving the new destination file to the country folder
    dummy_driver = get_url('https://www.google.com', dummy_driver)
    doc = drive_service.files().get(fileId=doc_id,
                                fields='parents').execute()
    previous_parents = ",".join(doc.get('parents'))
    doc = drive_service.files().update(fileId=doc_id, removeParents=previous_parents, addParents=dest_folder_id, fields='id, parents').execute()

    # sharing the doc
    request = {'role':'writer', 'type':'anyone'}
    dummy_driver = get_url('https://www.google.com', dummy_driver)
    drive_service.permissions().create(fileId=doc_id, body=request).execute()
    link_response = drive_service.files().get(fileId=doc_id, fields='webViewLink').execute()
    link = link_response['webViewLink']

    return link, folder_stamp


def pre_start_on_thread():

    global root, running, google_sheet_enter_field, hotels_limit_enter_field_doc, hotels_limit_enter_field_csv, google_drive_enter_field_csv, google_drive_enter_field_doc, thread
    if running == False:
        running = True
        return

    # resizing the UI window size to focus on the output
    root.geometry("900x700")
    # getting the arguments to be passed to the scraping function
    url = google_sheet_enter_field.get()
    doc_limit = hotels_limit_enter_field_doc.get()
    csv_limit = hotels_limit_enter_field_csv.get()
    doc_folder = google_drive_enter_field_doc.get()
    csv_folder = google_drive_enter_field_csv.get()  
    prop_type = types.get()
    rating = guest_rating.get()
    curr = currency.get()
    min_price = price1.get()
    max_price = price2.get()
    date1 = month1.get()
    date2 = month2.get()
    date3 = month3.get()
    date4 = month4.get()
    butt1 = Checkbutton1.get()
    butt2 = Checkbutton2.get()
    butt3 = Checkbutton3.get()
    butt4 = Checkbutton4.get()
    butt5 = Checkbutton5.get()
    butt6 = Checkbutton6.get()
    butt7 = Checkbutton7.get()
    order = hotels_order.get()
    # saving the settings to the dll file 
    data = {"google_sheet": url, "hotels_limit_docs": doc_limit, "hotels_limit_csv": csv_limit, 'password': True, "google_drive_docs": doc_folder, "google_drive_csv": csv_folder, "prop_type":prop_type, "rating":rating, "curr":curr, "min_price":min_price, "max_price":max_price, "date1":date1, "date2":date2, "date3":date3, "date4":date4, "butt1":butt1, "butt2":butt2, "butt3":butt3, "butt4":butt4, "butt5":butt5, "butt6":butt6, "butt7":butt7, "order":order}
    with open("settings.dll", "wb") as f:
        pickle.dump(data, f)

    button_start["state"] = "disabled"
    toplevel = tkinter.Frame(root)
    toplevel.pack(ipadx=0, ipady=0, anchor=tkinter.NE, fill=tkinter.Y, expand=True)
    LoggerGUI(toplevel)    

    # calling the scraper function
    thread = threading.Thread(target=scrape_hotels)
    thread.start()


def on_quit():
    global root, dummy_driver
    elapsed = round((time.time() - start)/60, 2)
    hours = round(elapsed/60, 2)
    output_msg(f'The bot is manually terminated by the user. Elapsed Time: {elapsed} mins ({hours} hours)', 1)
    root.destroy()
    sys.exit()

class LoggerGUI(tkinter.Frame):
    def __init__(self, parent, *args, **kwargs):
        tkinter.Frame.__init__(self, parent, *args, **kwargs)
        self.root = parent
        self.build_gui()

    def build_gui(self):
        path = os.getcwd()
        #self.root.geometry("750x300")
        #self.root.title(path)
        #self.root.attributes("-topmost", True)
        #self.root.option_add('*tearOff', 'FALSE')
        self.grid(column=0, row=0, sticky=tkinter.NSEW)
        self.grid_columnconfigure(0, weight=1, uniform='a')
        self.grid_columnconfigure(1, weight=1, uniform='a')
        self.grid_columnconfigure(2, weight=1, uniform='a')
        self.grid_columnconfigure(3, weight=1, uniform='a')
        st = ScrolledText.ScrolledText(self, state='disabled', width=100, height=35)

        st.configure(font='TkFixedFont')
        st.grid(column=0, row=1, sticky=tkinter.NSEW, columnspan=4)
        text_handler = TextHandler(st)

        logging.basicConfig(level=logging.INFO)

        logger = logging.getLogger()
        logger.addHandler(text_handler)
        print = logger.info
        logger.setLevel(20)
         # disabling google API messages
        logging.getLogger("googleapiclient.discovery").setLevel(30)      
        logging.getLogger("google.auth.transport.requests").setLevel(30)      
        logging.getLogger("google.oauth2.credentials").setLevel(30)      
        logging.getLogger("google_auth_oauthlib.flow").setLevel(30)      
        logging.getLogger("googleapiclient.http").setLevel(30)

def load_settings():
    try:
        with open("settings.dll", "rb") as file:
            data = pickle.load(file)
        return data
    except FileNotFoundError:
        return {}

class TextHandler(logging.Handler):
    global start
    def __init__(self, text):
        logging.Handler.__init__(self)
        self.text = text

    def emit(self, record):
        msg = self.format(record)

        def append():
            self.text.configure(state='normal')
            self.text.insert(tkinter.END, msg + '\n')
            self.text.configure(state='disabled')
            self.text.yview(tkinter.END)

        try:
            self.text.after(0, append)
        except Exception as err:
            sys.exit()


def run_GUI():
    # GUI
    global root, running, button_start, google_sheet_enter_field, hotels_limit_enter_field_doc, hotels_limit_enter_field_csv, google_drive_enter_field_doc, google_drive_enter_field_csv, price1, price2, currency, month1, month2, month3, month4, types, Checkbutton1, Checkbutton2, Checkbutton3, Checkbutton4, Checkbutton5, Checkbutton6, Checkbutton7, guest_rating, hotels_order, version, stamp, thread
    running = False
    # check settings dll file
    data = load_settings()
    # configuring the UI main window
    root = ThemedTk('breeze')
    root.resizable(True, True)
    root.protocol("WM_DELETE_WINDOW", on_quit)
    root.title(f"Hotels Scraper by Abdelrahman Hekal v{version}")
    root.geometry("1700x600")
    root.attributes('-disabled', True)
    root.attributes("-topmost", False)
    # check user one time password
    root.attributes("-topmost", True)
    root.attributes('-disabled', False)
    
    # stying the window
    styles = ttk.Style()
    styles.configure("rights.TLabel", font="Verdana 10")
    styles.configure("TLabel", padding=1, font="Verdana 10")
    styles.theme_use('breeze')

    # google sheet user inputs
    ttk.Label(root, text="URL For Google Sheet").place(relx=0.01, rely=0.1, anchor='w')
    google_sheet_enter_field = ttk.Entry(root)
    google_sheet_enter_field.place(relx=0.11, rely=0.1, relwidth=0.35, relheight=0.07, anchor='w')
    try:
        google_sheet_enter_field.insert(tkinter.END, data["google_sheet"])
    except Exception as err:
        pass   
        
    # google drive path to store the doc files
    ttk.Label(root, text="URL For Google Docs").place(relx=0.01, rely=0.18, anchor='w')
    google_drive_enter_field_doc = ttk.Entry(root)
    google_drive_enter_field_doc.place(relx=0.11, rely=0.18, relwidth=0.35, relheight=0.07, anchor='w')
    try:
        google_drive_enter_field_doc.insert(tkinter.END, data["google_drive_docs"])
    except Exception as err:
        pass   
        
    # google drive path to store the doc files
    ttk.Label(root, text="URL For CSV Files").place(relx=0.01, rely=0.26, anchor='w')
    google_drive_enter_field_csv = ttk.Entry(root)
    google_drive_enter_field_csv.place(relx=0.11, rely=0.26, relwidth=0.35, relheight=0.07, anchor='w')
    try:
        google_drive_enter_field_csv.insert(tkinter.END, data["google_drive_csv"])
    except Exception as err:
        pass

    # hotels limit in google doc
    ttk.Label(root, text="Google Doc Limit").place(relx=0.01, rely=0.36, anchor='w')
    hotels_limit_enter_field_doc = ttk.Entry(root)
    hotels_limit_enter_field_doc.place(relx=0.11, rely=0.36, relwidth=0.05, relheight=0.07, anchor='center')
    try:
        hotels_limit_enter_field_doc.insert(tkinter.END, data["hotels_limit_docs"])
    except Exception as err:
        pass       
       
    # hotels limit in the csv file
    ttk.Label(root, text="CSV File Limit").place(relx=0.15, rely=0.36, anchor='w')
    hotels_limit_enter_field_csv = ttk.Entry(root)
    hotels_limit_enter_field_csv.place(relx=0.24, rely=0.36, relwidth=0.05, relheight=0.07, anchor='center')
    try:
        hotels_limit_enter_field_csv.insert(tkinter.END, data["hotels_limit_csv"])
    except Exception as err:
        pass      
        
    ttk.Label(root, text="Min Price Per Night").place(relx=0.192, rely=0.45, anchor='w')
    price1 = ttk.Entry(root)
    price1.place(relx=0.295, rely=0.45, relwidth=0.05, relheight=0.07, anchor='center')
    try:
        price1.insert(tkinter.END, data["min_price"])
    except Exception as err:
        pass      
    ttk.Label(root, text="Max Price Per Night").place(relx=0.327, rely=0.45, anchor='w')
    price2 = ttk.Entry(root)
    price2.place(relx=0.435, rely=0.45, relwidth=0.05, relheight=0.07, anchor='center')
    try:
        price2.insert(tkinter.END, data["max_price"])
    except Exception as err:
        pass         
    #currency
    ttk.Label(root, text="Currency").place(relx=0.01, rely=0.45, anchor='w')  
    # Dropdown menu options
    curr_options = ["US Dollar (USD)", "Australian Dollar (AUD)", "British Pound (GBP)", "Euro (EUR)", "Chinese Yuan (CNY)", "Indian Rupee (INR)", "Israeli Shekel (ILS)", "Thai Baht (THB)"]
    # datatype of menu text
    currency = tkinter.StringVar()
    # initial menu text
    try:
        currency.set(data["curr"])
    except Exception as err:
        currency.set("US Dollar (USD)")
    curr_menu = tkinter.OptionMenu(root , currency, *curr_options)
    curr_menu.place(relx=0.125, rely=0.45, relwidth=0.115, relheight=0.07, anchor='center')   
        
    #pricing month1
    ttk.Label(root, text="Month 1").place(relx=0.01, rely=0.55, anchor='w')  
    # Dropdown menu options
    month_options = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December", 'Disable']
    # datatype of menu text
    month1 = tkinter.StringVar()
    # initial menu text
    try:
        month1.set(data["date1"])
    except Exception as err:
        month1.set("January")
    
    menu1 = tkinter.OptionMenu(root , month1, *month_options)
    menu1.place(relx=0.081, rely=0.55, relwidth=0.07, relheight=0.07, anchor='center')
  
    #pricing month2
    ttk.Label(root, text="Month 2").place(relx=0.125, rely=0.55, anchor='w')  
    # Dropdown menu options
    month_options = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December", 'Disable']
    # datatype of menu text
    month2 = tkinter.StringVar()
    # initial menu text
    try:
        month2.set(data["date2"])
    except Exception as err:
        month2.set("April")
    
    menu2 = tkinter.OptionMenu(root , month2, *month_options)
    menu2.place(relx=0.195, rely=0.55, relwidth=0.07, relheight=0.07, anchor='center')     
        
    #pricing month3
    ttk.Label(root, text="Month 3").place(relx=0.235, rely=0.55, anchor='w')  
    # Dropdown menu options
    month_options = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December", 'Disable']
    # datatype of menu text
    month3 = tkinter.StringVar()
    # initial menu text
    try:
        month3.set(data["date3"])
    except Exception as err:
        month3.set("July")
    
    menu3 = tkinter.OptionMenu(root , month3, *month_options)
    menu3.place(relx=0.307, rely=0.55, relwidth=0.07, relheight=0.07, anchor='center')   
        
    #pricing month4
    ttk.Label(root, text="Month 4").place(relx=0.353, rely=0.55, anchor='w')  
    # Dropdown menu options
    month_options = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December", 'Disable']
    # datatype of menu text
    month4 = tkinter.StringVar()
    # initial menu text
    try:
        month4.set(data["date4"])
    except Exception as err:
        month4.set("October")
    
    menu4 = tkinter.OptionMenu(root , month4, *month_options)
    menu4.place(relx=0.425, rely=0.55, relwidth=0.07, relheight=0.07, anchor='center')

    # hotels order in the csv file
    ttk.Label(root, text="Order In Google Doc").place(relx=0.29, rely=0.36, anchor='w')  
    # Dropdown menu options
    order_options = ["Descending", "Ascending"]
    # datatype of menu text
    hotels_order = tkinter.StringVar()
    # initial menu text
    try:
        hotels_order.set(data["order"])
    except Exception as err:
        hotels_order.set("Ascending")

    hotels_order_menu = tkinter.OptionMenu(root , hotels_order, *order_options)
    hotels_order_menu.place(relx=0.42, rely=0.36, relwidth=0.08, relheight=0.07, anchor='center') 
        
    #class
    ttk.Label(root, text="Hotel Class").place(relx=0.01, rely=0.65, anchor='w')
    try:
        Checkbutton1 = tkinter.IntVar(value=data["butt1"])
    except Exception as err:
        Checkbutton1 = tkinter.IntVar(value=1)    
    try:
        Checkbutton2 = tkinter.IntVar(value=data["butt2"])
    except Exception as err:
        Checkbutton2 = tkinter.IntVar(value=0)    
    try:
        Checkbutton3 = tkinter.IntVar(value=data["butt3"])
    except Exception as err:
        Checkbutton3 = tkinter.IntVar(value=0)   
    try:
        Checkbutton4 = tkinter.IntVar(value=data["butt4"])
    except Exception as err:
        Checkbutton4 = tkinter.IntVar(value=0)    
    try:
        Checkbutton5 = tkinter.IntVar(value=data["butt5"])
    except Exception as err:
        Checkbutton5 = tkinter.IntVar(value=0)


    Button1 = tkinter.Checkbutton(root, text = "Any", variable = Checkbutton1, onvalue = 1, offvalue = 0)
    Button1.place(relx=0.12, rely=0.65, relwidth=0.05, relheight=0.07, anchor='center')    
    Button2 = tkinter.Checkbutton(root, text = "2-stars", variable = Checkbutton2, onvalue = 1, offvalue = 0)
    Button2.place(relx=0.19, rely=0.65, relwidth=0.05, relheight=0.07, anchor='center')    
    Button3 = tkinter.Checkbutton(root, text = "3-stars", variable = Checkbutton3, onvalue = 1, offvalue = 0)
    Button3.place(relx=0.27, rely=0.65, relwidth=0.05, relheight=0.07, anchor='center')    
    Button4 = tkinter.Checkbutton(root, text = "4-stars", variable = Checkbutton4, onvalue = 1, offvalue = 0)
    Button4.place(relx=0.35, rely=0.65, relwidth=0.05, relheight=0.07, anchor='center')    
    Button5 = tkinter.Checkbutton(root, text = "5-stars", variable = Checkbutton5, onvalue = 1, offvalue = 0)
    Button5.place(relx=0.43, rely=0.65, relwidth=0.05, relheight=0.07, anchor='center')
 
    #guest rating
    ttk.Label(root, text="Guest Rating").place(relx=0.01, rely=0.73, anchor='w')  
    # Dropdown menu options
    rating_options = ["Any", "3.5", "4.0", "4.5"]
    # datatype of menu text
    guest_rating = tkinter.StringVar()
    # initial menu text
    try:
        guest_rating.set(data["rating"])
    except Exception as err:
        guest_rating.set("Any")
    
    rating_menu = tkinter.OptionMenu(root, guest_rating, *rating_options)
    rating_menu.place(relx=0.09, rely=0.73, relwidth=0.05, relheight=0.07, anchor='center')

    # property type
    ttk.Label(root, text="Property Type").place(relx=0.13, rely=0.73, anchor='w')  
    # Dropdown menu options
    types_options = ["Hotels", "Holiday rentals"]
    # datatype of menu text
    types = tkinter.StringVar()
    # initial menu text
    try:
        types.set(data["prop_type"])
    except Exception as err:
        types.set("Hotels")
    
    type_menu = tkinter.OptionMenu(root , types, *types_options)
    type_menu.place(relx=0.235, rely=0.73, relwidth=0.09, relheight=0.07, anchor='center') 

    # show prices in google doc 
    try:
        Checkbutton6 = tkinter.IntVar(value=data["butt6"])
    except Exception as err:
        Checkbutton6 = tkinter.IntVar(value=0)
    Button6 = tkinter.Checkbutton(root, text = "Include Prices In Google Doc", variable = Checkbutton6, onvalue = 1, offvalue = 0)
    Button6.place(relx=0.07, rely=0.82, relwidth=0.15, relheight=0.07, anchor='center')      
    
    # show hotel website in google doc 
    try:
        Checkbutton7 = tkinter.IntVar(value=data["butt7"])
    except Exception as err:
        Checkbutton7 = tkinter.IntVar(value=0)
    Button7 = tkinter.Checkbutton(root, text = "Include Websites In Google Doc", variable = Checkbutton7, onvalue = 1, offvalue = 0)
    Button7.place(relx=0.23, rely=0.82, relwidth=0.15, relheight=0.07, anchor='center')    

    # status bar
    path = os.getcwd() + f" - {stamp}"
    ttk.Label(root, text=path, relief=tkinter.SUNKEN, anchor=tkinter.W).pack(side=tkinter.BOTTOM, fill=tkinter.X)

    button_start = ttk.Button(root, text="Start", command= pre_start_on_thread)
    button_start.place(relx=0.22, rely=0.9, relwidth=0.1, relheight=0.07, anchor='center')
    button_start.invoke()
    root.mainloop()

def clear_screen():
    # for windows
    if os.name == 'nt':
        _ = os.system('cls')
  
    # for mac and linux
    else:
        _ = os.system('clear')

def scrape_hotels():
    
    global running, root, start, driver, version, dummy_driver, driver_path

    # hardwired inputs for development and debbugging
    #url = 'https://docs.google.com/spreadsheets/d/1x0zbSr8mGQhPt3kiA-HcWpbBfC5hzSV4yd-PHRB-8H8'
    #doc_limit = 100
    #csv_limit = ''
    #doc_folder = 'https://drive.google.com/drive/folders/1xzFqqE2DixTEnPLoHTWi9E9iUSOHj4-Z'  
    #csv_folder = 'https://drive.google.com/drive/folders/1xzFqqE2DixTEnPLoHTWi9E9iUSOHj4-Z'  
    #prop_type = 'Hotels'
    #prop_class = 'Any'
    #rating = 'Any'
    #curr = 'USD'
    #min_price = -1
    #max_price = -1
    ####################################################
    # getting user inputs from the UI
    url = google_sheet_enter_field.get()
    doc_limit = hotels_limit_enter_field_doc.get()
    csv_limit = hotels_limit_enter_field_csv.get()
    doc_folder = google_drive_enter_field_doc.get()
    csv_folder = google_drive_enter_field_csv.get()
    prop_type = types.get()
    rating = guest_rating.get()
    curr = currency.get()
    curr = curr[curr.find('(')+1:curr.find(')')]
    min_price = price1.get()
    max_price = price2.get()
    date1 = month1.get()
    date2 = month2.get()
    date3 = month3.get()
    date4 = month4.get()
    prop_class = []
    if Checkbutton1.get() == 1:
        prop_class.append('Any')    
    if Checkbutton2.get() == 1:
        prop_class.append('2-star')    
    if Checkbutton3.get() == 1:
        prop_class.append('3-star')    
    if Checkbutton4.get() == 1:
        prop_class.append('4-star')    
    if Checkbutton5.get() == 1:
        prop_class.append('5-star')

    if Checkbutton6.get() == 1:
        prices = True
    else:
        prices = False    
        
    if Checkbutton7.get() == 1:
        website = True
    else:
        website = False

    order = hotels_order.get()
    # validating the google sheet url input
    if len(url) == 0 or 'docs.google.com/spreadsheets/d' not in url.lower():
        output_msg("Invalid Google sheet link, please try again!", 1)
        sys.exit()     
        
    # validating the google docs folder ID input
    if len(doc_folder) == 0 or 'https://drive.google.com/drive/' not in doc_folder.lower():
        output_msg("Invalid Google docs drive folder link, please try again!", 1)
        sys.exit()       
        
        # validating the csv folder ID input
    if len(csv_folder) == 0 or 'https://drive.google.com/drive/' not in csv_folder.lower():
        output_msg("Invalid csv drive folder link, please try again!", 1)
        sys.exit()   

    doc_folder = doc_folder.split('/')[-1].split('?')[0]
    csv_folder = csv_folder.split('/')[-1].split('?')[0]

    try:
        doc_limit = int(doc_limit)
    except Exception as err:
        output_msg("Invalid Google docs Limit Input, please try again!", 1)
        sys.exit()  
    if doc_limit < 1:
        output_msg("Invalid Google docs Limit Input, please try again!", 1)     
        sys.exit() 
              
    if csv_limit != '':
        try:
            csv_limit = int(csv_limit)
        except Exception as err:
            output_msg("Invalid csv Limit Input, please try again!", 1)
            sys.exit()  
        if csv_limit < 1:
            output_msg("Invalid csv Limit Input, please try again!", 1)
            sys.exit()   
            
    if min_price != '':
        try:
            min_price = int(min_price)
        except Exception as err:
            output_msg("Invalid Min Price Per Night Input, please try again!", 1)
            sys.exit()  
        if min_price < 1:
            output_msg("Min Price Per Night Input Must Be Greater Than Zero, please try again!", 1) 
            sys.exit()     
            
    if max_price != '':
        try:
            max_price = int(max_price)
        except Exception as err:
            output_msg("Invalid Min Price Per Night Input, please try again!", 1)
            sys.exit()  
        if max_price < 1:
            output_msg("Max Price Per Night Input Must Be Greater Than Zero, please try again!", 1) 
            sys.exit()  

    # log file creation
    if os.path.exists(os.getcwd() + f'\\session_log_{stamp}.log'):
             os.remove(os.getcwd() + f'\\session_log_{stamp}.log') 

    start = time.time()
    output_msg(f'Starting the bot v{version}', 1)
    output_msg('-'*75, 0)
    dests = process_sheet(url)
    for k, dest in enumerate(dests):
        # skip destinations or hotels with status mentioned
        if len(dest[1]) > 0:
            output_msg(f'Destination {dest[0].title()} has status "{dest[1]}", skipping...', 1)
            continue

        scraped = False
        # scraping hotels data
        try:
            dummy_driver = get_url('https://www.google.com', dummy_driver)
            df, dest = search_hotels(url, dest, k, prop_type, prop_class, rating, min_price, max_price, curr, csv_limit, date1, date2, date3, date4)
            if df.shape[0] > 0:
                scraped = True
        except Exception as err:
            write_status(k+2, str(err).split('Backtrace')[0], url)
            output_msg(f'the below error occurred in scraping the hotels for destination {dest[0].title()}, skipping ...', 1)
            err = str(err)
            output_msg(err, 0)
        
        success = False
        if scraped:
            # getting google location for the hotels
            try:
                csv_time = time.time()
                dummy_driver = get_url('https://www.google.com', dummy_driver)
                df2, folder_stamp = get_google_loc(df, csv_folder, dest, prop_type, url, k)
                if df2.shape[0] > 0:
                    elapsed = round((time.time() - csv_time)/60, 2)
                    hours = round(elapsed/60, 2)
                    output_msg(f'CSV File Generation For {dest[0].title()} Is Completed Successfully! \nElaspsed Time: {elapsed} mins ({hours} hours)', 1)
                    success = True
            except Exception as err:
                write_status(k+2, str(err).split('Backtrace')[0], url)
                output_msg(f'the below error occurred in scraping the {prop_type} for destination {dest[0].title()}, skipping ...', 1)
                err = str(err)
                output_msg(err, 0)

        if success:
            # creating Google doc for the hotels
            for z in range(2):
                try:
                    dummy_driver = get_url('https://www.google.com', dummy_driver)
                    doc_time = time.time()
                    status = create_hotel_doc(df2, dest, doc_folder, doc_limit, curr, date1, date2, date3, date4, order, prices, website, prop_type, folder_stamp)
                    if len(status) == 0:
                        # successfully processed the destination
                        write_status(k+2, 'Scraped', url)
                        output_msg(f'Process completed successfully for: {dest[0].title()}', 1)       
                    elif status != '0':
                        # failure in the destination procesing
                        write_status(k+2, status, url)
                        output_msg(f'the following error occurred in exporting Google doc for destination {dest[0].title()}, skipping ...', 1)
                        output_msg(status, 0)
                    elapsed = round((time.time() - doc_time)/60, 2)
                    hours = round(elapsed/60, 2)
                    output_msg(f'Google Docs Creation Process For {dest[0].title()} Is Completed Successfully!\nElapsed Time: {elapsed} mins ({hours} hours)', 1)
                    break
                except Exception as err:
                    if z == 0:
                        output_msg(f'the below error occurred in exporting Google doc for destination {dest[0].title()}, retrying ...', 1)
                        output_msg(err, 0)
                    else:
                        output_msg(f'the below error occurred in exporting Google doc for destination {dest[0].title()}, skipping ...', 1)
                        output_msg(err, 0)

    dummy_driver.quit()
    elapsed = round((time.time() - start)/60, 2)
    hours = round(elapsed/60, 2)
    output_msg(f'All the destinations in the Google sheet are Processed! Elapsed Time: {elapsed} mins ({hours} hours)', 1)
    sys.exit()


# main program      
if __name__ == '__main__':

    global version, dummy_driver , driver_path, stamp
    version = '1.6'
    stamp = datetime.now().strftime("%d-%m-%Y-%H-%M")
    driver_path = ChromeDriverManager().install()
    dummy_driver = initialize_bot(driver_path)
    # running the UI function
    run_GUI()