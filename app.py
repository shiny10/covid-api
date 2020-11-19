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
    "1": 164, 
    "2": 3, 
    "3": 4282, 
    "4": 15, 
    "5": 61, 
    "6": 1.0, 
    "7": "hi"
  }, 
  "1": {
    "0": 2, 
    "1": 20857, 
    "2": 58, 
    "3": 822011, 
    "4": 1777, 
    "5": 6837, 
    "6": 9.0, 
    "7": "Andhra Pradesh"
  }, 
  "2": {
    "0": 3, 
    "1": 1440, 
    "2": 45, 
    "3": 14214, 
    "4": 88, 
    "5": 47, 
    "6": 1.0, 
    "7": "Arunachal Pradesh                                 "
  }, 
  "3": {
    "0": 4, 
    "1": 4799, 
    "2": 572, 
    "3": 204079, 
    "4": 771, 
    "5": 957, 
    "6": 3.0, 
    "7": "Assam                                             "
  }, 
  "4": {
    "0": 5, 
    "1": 5763, 
    "2": 52, 
    "3": 217594, 
    "4": 654, 
    "5": 1167, 
    "6": 5.0, 
    "7": "Bihar                                             "
  }, 
  "5": {
    "0": 6, 
    "1": 1002, 
    "2": 44, 
    "3": 14297, 
    "4": 64, 
    "5": 244, 
    "6": 1.0, 
    "7": "Chandigarh                                        "
  }, 
  "6": {
    "0": 7, 
    "1": 20061, 
    "2": 165, 
    "3": 185152, 
    "4": 1962, 
    "5": 2527, 
    "6": 20.0, 
    "7": "Chhattisgarh                                      "
  }, 
  "7": {
    "0": 8, 
    "1": 61, 
    "2": 165, 
    "3": 185152, 
    "4": 1962, 
    "5": 2527, 
    "6": 20.0, 
    "7": "Dadra and Nagar Haveli and Daman and Diu          "
  }, 
  "8": {
    "0": 9, 
    "1": 43116, 
    "2": 487, 
    "3": 416580, 
    "4": 6462, 
    "5": 7332, 
    "6": 104.0, 
    "7": "Delhi                                             "
  }, 
  "9": {
    "0": 11, 
    "1": 12321, 
    "2": 98, 
    "3": 168858, 
    "4": 1175, 
    "5": 3785, 
    "6": 9.0, 
    "7": "Gujarat                                           "
  }, 
  "10": {
    "0": 12, 
    "1": 18867, 
    "2": 754, 
    "3": 172265, 
    "4": 2015, 
    "5": 1979, 
    "6": 19.0, 
    "7": "Haryana                                           "
  }, 
  "11": {
    "0": 13, 
    "1": 6165, 
    "2": 560, 
    "3": 21607, 
    "4": 199, 
    "5": 411, 
    "6": 6.0, 
    "7": "Himachal Pradesh                                  "
  }, 
  "12": {
    "0": 14, 
    "1": 5578, 
    "2": 98, 
    "3": 93824, 
    "4": 511, 
    "5": 1566, 
    "6": 8.0, 
    "7": "Jammu and Kashmir                                 "
  }, 
  "13": {
    "0": 15, 
    "1": 3668, 
    "2": 341, 
    "3": 100908, 
    "4": 606, 
    "5": 917, 
    "6": 4.0, 
    "7": "Jharkhand                                         "
  }, 
  "14": {
    "0": 29, 
    "1": 264, 
    "2": 27, 
    "3": 4019, 
    "4": 48, 
    "5": 85, 
    "6": 3.0, 
    "7": "Sikkim                                            "
  }, 
  "15": {
    "0": 28, 
    "1": 17352, 
    "2": 359, 
    "3": 199943, 
    "4": 1804, 
    "5": 2032, 
    "6": 13.0, 
    "7": "Rajasthan                                         "
  }, 
  "16": {
    "0": 27, 
    "1": 5439, 
    "2": 193, 
    "3": 130018, 
    "4": 469, 
    "5": 4412, 
    "6": 23.0, 
    "7": "Punjab                                            "
  }, 
  "17": {
    "0": 26, 
    "1": 1071, 
    "2": 6, 
    "3": 34501, 
    "4": 69, 
    "5": 607, 
    "6": 2.0, 
    "7": "Puducherry                                        "
  }, 
  "18": {
    "0": 25, 
    "1": 10762, 
    "2": 292, 
    "3": 293741, 
    "4": 1264, 
    "5": 1483, 
    "6": 14.0, 
    "7": "Odisha                                            "
  }, 
  "19": {
    "0": 24, 
    "1": 789, 
    "2": 52, 
    "3": 8776, 
    "4": 89, 
    "5": 50, 
    "6": 100, 
    "7": "Nagaland                                          "
  }, 
  "20": {
    "0": 23, 
    "1": 568, 
    "2": 16, 
    "3": 2739, 
    "4": 51, 
    "5": 2, 
    "6": 50, 
    "7": "Mizoram                                           "
  }, 
  "21": {
    "0": 22, 
    "1": 1045, 
    "2": 83, 
    "3": 9368, 
    "4": 56, 
    "5": 98, 
    "6": 4.0, 
    "7": "Meghalaya                                         "
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