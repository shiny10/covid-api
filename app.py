#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This code scraps mohfw.gov.in for COVID-19 Data

"""
import requests
import json
import logging
import threading

from flask import Flask

from flask import jsonify

from bs4 import BeautifulSoup
import pandas as pd
from flask import Flask, jsonify
from datetime import datetime
import sys

app = Flask(__name__)


# Removed Foreign National Column


data = {
  "0": {
    "0": 1, 
    "1": 145, 
    "2": 9, 
    "3": 4398, 
    "4": 20.0, 
    "5": 61, 
    "6": 'NaN', 
    "7": "Andaman and Nicobar Islands"
  }, 
  "1": {
    "0": 2, 
    "1": 16000, 
    "2": 516, 
    "3": 835801, 
    "4": 1821.0, 
    "5": 6910, 
    "6": 11.0, 
    "7": "Andhra Pradesh"
  }, 
  "2": {
    "0": 3, 
    "1": 1127, 
    "2": 55, 
    "3": 14800, 
    "4": 85.0, 
    "5": 49, 
    "6": 1.0, 
    "7": "Arunachal Pradesh"
  }, 
  "3": {
    "0": 4, 
    "1": 3193, 
    "2": 92, 
    "3": 206878, 
    "4": 267.0, 
    "5": 969, 
    "6": 'NaN', 
    "7": "Assam"
  }, 
  "4": {
    "0": 5, 
    "1": 5378, 
    "2": 29, 
    "3": 221924, 
    "4": 652.0, 
    "5": 1209, 
    "6": 8.0, 
    "7": "Bihar"
  }, 
  "5": {
    "0": 6, 
    "1": 1105, 
    "2": 24, 
    "3": 14963, 
    "4": 130.0, 
    "5": 254, 
    "6": 1.0, 
    "7": "Chandigarh"
  }, 
  "6": {
    "0": 7, 
    "1": 19421, 
    "2": 651, 
    "3": 195469, 
    "4": 1472.0, 
    "5": 2672, 
    "6": 26.0, 
    "7": "Chhattisgarh"
  }, 
  "7": {
    "0": 8, 
    "1": 31, 
    "2": 2, 
    "3": 3274, 
    "4": 'NaN', 
    "5": 2, 
    "6": 'NaN', 
    "7": "Dadra and Nagar Haveli and Daman and Diu"
  }, 
  "8": {
    "0": 9, 
    "1": 43221, 
    "2": 763, 
    "3": 459368, 
    "4": 6685.0, 
    "5": 8041, 
    "6": 98.0, 
    "7": "Delhi"
  }, 
  "9": {
    "0": 10, 
    "1": 1343, 
    "2": 21, 
    "3": 44467, 
    "4": 157.0, 
    "5": 670, 
    "6": 'NaN', 
    "7": "Goa"
  }, 
  "10": {
    "0": 11, 
    "1": 12677, 
    "2": 220, 
    "3": 176475, 
    "4": 1113.0, 
    "5": 3830, 
    "6": 7.0, 
    "7": "Gujarat"
  }, 
  "11": {
    "0": 12, 
    "1": 19579, 
    "2": 36, 
    "3": 187559, 
    "4": 2156.0, 
    "5": 2113, 
    "6": 20.0, 
    "7": "Haryana"
  }, 
  "12": {
    "0": 13, 
    "1": 6980, 
    "2": 79, 
    "3": 24729, 
    "4": 704.0, 
    "5": 488, 
    "6": 13.0, 
    "7": "Himachal Pradesh"
  }, 
  "13": {
    "0": 14, 
    "1": 5560, 
    "2": 10, 
    "3": 97537, 
    "4": 565.0, 
    "5": 1618, 
    "6": 5.0, 
    "7": "Jammu and Kashmir"
  }, 
  "14": {
    "0": 15, 
    "1": 2600, 
    "2": 37, 
    "3": 103435, 
    "4": 264.0, 
    "5": 937, 
    "6": 3.0, 
    "7": "Jharkhand"
  }, 
  "15": {
    "0": 16, 
    "1": 25188, 
    "2": 23, 
    "3": 830988, 
    "4": 1800.0, 
    "5": 11604, 
    "6": 26.0, 
    "7": "Karnataka"
  }, 
  "16": {
    "0": 17, 
    "1": 68352, 
    "2": 1164, 
    "3": 475320, 
    "4": 6860.0, 
    "5": 1969, 
    "6": 26.0, 
    "7": "Kerala"
  }, 
  "17": {
    "0": 18, 
    "1": 1000, 
    "2": 76, 
    "3": 6619, 
    "4": 14.0, 
    "5": 95, 
    "6": 1.0, 
    "7": "Ladakh"
  }, 
  "18": {
    "0": 19, 
    "1": 9800, 
    "2": 462, 
    "3": 175089, 
    "4": 887.0, 
    "5": 3129, 
    "6": 14.0, 
    "7": "Madhya Pradesh"
  }, 
  "19": {
    "0": 20, 
    "1": 80728, 
    "2": 479, 
    "3": 1635971, 
    "4": 5860.0, 
    "5": 46356, 
    "6": 154.0, 
    "7": "Maharashtra"
  }, 
  "20": {
    "0": 21, 
    "1": 2886, 
    "2": 58, 
    "3": 19431, 
    "4": 172.0, 
    "5": 231, 
    "6": 'NaN', 
    "7": "Manipur"
  }, 
  "21": {
    "0": 22, 
    "1": 801, 
    "2": 48, 
    "3": 10074, 
    "4": 60.0, 
    "5": 104, 
    "6": 1.0, 
    "7": "Meghalaya"
  }, 
  "22": {
    "0": 23, 
    "1": 511, 
    "2": 24, 
    "3": 3025, 
    "4": 4.0, 
    "5": 5, 
    "6": 'NaN', 
    "7": "Mizoram"
  }, 
  "23": {
    "0": 24, 
    "1": 1296, 
    "2": 41, 
    "3": 9110, 
    "4": 53.0, 
    "5": 54, 
    "6": 'NaN', 
    "7": "Nagaland"
  }, 
  "24": {
    "0": 25, 
    "1": 7400, 
    "2": 168, 
    "3": 302796, 
    "4": 1019.0, 
    "5": 1592, 
    "6": 17.0, 
    "7": "Odisha"
  }, 
  "25": {
    "0": 26, 
    "1": 670, 
    "2": 35, 
    "3": 35254, 
    "4": 102.0, 
    "5": 609, 
    "6": 1.0, 
    "7": "Puducherry"
  }, 
  "26": {
    "0": 27, 
    "1": 194, 
    "2": 257, 
    "3": 133427, 
    "4": 510.0, 
    "5": 4556, 
    "6": 15.0, 
    "7": "Punjab"
  }, 
  "27": {
    "0": 28, 
    "1": 20168, 
    "2": 690, 
    "3": 212623, 
    "4": 1844.0, 
    "5": 2116, 
    "6": 15.0, 
    "7": "Rajasthan"
  }, 
  "28": {
    "0": 29, 
    "1": 296, 
    "2": 4, 
    "3": 4218, 
    "4": 36.0, 
    "5": 95, 
    "6": 'NaN', 
    "7": "Sikkim"
  }, 
  "29": {
    "0": 30, 
    "1": 13907, 
    "2": 563, 
    "3": 739532, 
    "4": 2251.0, 
    "5": 11550, 
    "6": 19.0, 
    "7": "Tamil Nadu"
  }, 
  "30": {
    "0": 31, 
    "1": 12515, 
    "2": 167, 
    "3": 247790, 
    "4": 1057.0, 
    "5": 1423, 
    "6": 4.0, 
    "7": "Telengana"
  }, 
  "31": {
    "0": 32, 
    "1": 959, 
    "2": 38, 
    "3": 30968, 
    "4": 112.0, 
    "5": 365, 
    "6": 1.0, 
    "7": "Tripura"
  }, 
  "32": {
    "0": 33, 
    "1": 4133, 
    "2": 14, 
    "3": 64427, 
    "4": 395.0, 
    "5": 1133, 
    "6": 5.0, 
    "7": "Uttarakhand"
  }, 
  "33": {
    "0": 34, 
    "1": 22757, 
    "2": 803, 
    "3": 488911, 
    "4": 1690.0, 
    "5": 7480, 
    "6": 39.0, 
    "7": "Uttar Pradesh"
  }, 
  "34": {
    "0": 35, 
    "1": 25873, 
    "2": 423, 
    "3": 411759, 
    "4": 3990.0, 
    "5": 7873, 
    "6": 53.0, 
    "7": "West Bengal"
  }
}

# Initialisations
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
app = Flask(__name__)


# config
FETCH_INTERVAL = 1800

# global response variable, to be overwritten later

@app.route('/api/get', methods=['GET'])
def get_tasks():
   return data
    

# scrapes table from the given url
def get_table_from_web():
    url = "http://mohfw.gov.in"
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    print(soup)
    div = soup.find('div', class_='data-table')
    table = div.find('table', class_='table')
    return table

# When provided with rows of a table, returns state_data, after cleaning
# Assumed that state data will be for first 35 rows in body of table
def get_state_wise_data(rows):
    rows = rows[:36]
    state_data = []
    for row in rows:
        cells = row.find_all("td")
        if len(cells) != 0 and len(cells) == len(headers):
            items = {}
            for index in headers:
                items[headers[index]] = cells[index].text.replace(
                    '\n', '').replace('#', '').replace('*','')
            state_data.append(items)
            insert_data(items)
    return state_data

# When provided with rows of a table, returns total_data, assuming that 36th row is total_data
def get_total_data(rows):
    total = rows[36].find_all("strong")
    total_data = {}
    for index in headers:
        if index != 0 and index != 1:
            total_data[headers[index]] = total[index-1].text
    return total_data

# get_data combines all the information given extracted table content
def get_data(content, time, indent=None):
    rows = content.find_all("tr")
    
    state_data = get_state_wise_data(rows) # consider all rows except the last 6
    
    total_data = get_total_data(rows)
    
    response = {
        "data": {
            "state_data": state_data,
            "total_data": total_data,
            "last_updated": str(time)
        }
    }
    
    return response


# parent function that calls the scraping function and get_data function
def data_extract():
    table = get_table_from_web()
    logging.info("Table fetched. \n Fetching state wise data from table...\n")
    
    state_wise_data = get_data(table, datetime.now())
    logging.info("Fetched state wise data.\n")
    
    global last_extracted_content
    #last_extracted_content = json.dumps(parsed, indent=4)

@app.route('/', methods=['GET'])
def home():
    return '''
    <h1>COVID 19 India Data</h1>'''

@app.route('/v1/api', methods=['GET'])
def api():
    global last_extracted_content
    logging.info("Request received, response: %s", last_extracted_content)
    return jsonify(last_extracted_content)

def start_thread():
    threading.Timer(FETCH_INTERVAL, data_extract, ()).start()

if __name__ == "__main__":
    logging.info("****** COVID-INDIA-API *******")
    app.run(debug=True)