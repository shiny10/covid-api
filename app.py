#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This code scraps mohfw.gov.in for COVID-19 Data

"""
import requests
import json
import logging
import threading
import sqlalchemy as db

from flask import Flask
import psycopg2

from flask import jsonify

from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine
from flask import Flask, jsonify
from datetime import datetime
import sys

app = Flask(__name__)

tasks = [
    {
        'id': 5,
        'title': u'hi this is shiny',
        'description': u'Milk, Cheese, Pizza, Fruit, Tylenol', 
        'done': False
    }
]

# Removed Foreign National Column
headers = {
    0: "id",
    1: "place",
    2: "active",
    3: "cured",
    4: "deaths",
    5: "total_confirmed"
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
    engine = create_engine('postgresql+psycopg2://postgres:shiny@10@localhost:5432/postgres')     
    connection = engine.connect()
    metadata = db.MetaData()
    statewise_data = db.Table('statewise_data', metadata, autoload=True, autoload_with=engine,port=5433)    
    query = db.select([statewise_data])  
    ResultProxy = connection.execute(query)
    ResultSet = ResultProxy.fetchall()
    df = pd.DataFrame(ResultSet)
    return df.to_dict('index')
    # with engine.begin() as conn:
    #     conn.execute(sql)
    # credentials = "postgresql://postgres:shiny@10@localhost:5432/postgres"
    # dataframe = pd.read_sql("SELECT * from statewise_data", con = credentials)
    # #print(dataframe)
    # result = dataframe.to_json(orient='records')
    # return result

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

def insert_data(items):
    sql = """INSERT INTO statewise_data VALUES(item.total,item.changesinceyest,item.cumulative,item.sinceyest,item.deathcum,item.deathsinceyest,item.statename) ;"""
    conn = None
    try:
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.executemany(sql,vendor_list)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

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