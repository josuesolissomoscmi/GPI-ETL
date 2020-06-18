import logging
from azure.storage.blob import BlockBlobService, PublicAccess
import requests
import urllib
from io import BytesIO
import openpyxl as xl
import xlrd
import time
import pandas as pd
import numpy as np
import pyodbc
import re
from bs4 import BeautifulSoup
import datetime
import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        r = upload_azure()
        return func.HttpResponse(r)
    else:
        return func.HttpResponse(
             "Please pass a name on the query string or in the request body",
             status_code=400
        )

sql_yahoo = "SELECT Symbol, MAX(Date) FROM [ST_YAHOO].[HISTORICAL_MARKET] GROUP BY Symbol"

def get_data(symbol,start,end):
    base_url = 'https://query1.finance.yahoo.com/v7/finance/download/'
    #EJ Symbol: ^GSPC
    base_url = base_url+symbol+'?%s'
    payload = urllib.parse.urlencode({'period1': start, 'period2': end, 'interval': '1d', 'events':'history'})

    req = urllib.request.urlopen(base_url % payload)
    req_read = req.read()
    file_download = BytesIO(req_read)

    df = pd.read_csv(file_download)

    df['Close'].replace('', np.nan, inplace=True)
    df.dropna(subset=['Close'], inplace=True)
    
    df['Symbol'] = symbol
    df['actualizacion'] = datetime.datetime.now()

    return df

def upload_azure():
    # Create the BlockBlockService that is used to call the Blob service for the storage account
    block_blob_service = BlockBlobService(account_name='gpistore', account_key='zfKM5R0PuPwR0F+pPsgs5BW/AQjAxv5fwKojoP2W38II++qfT6e+axFrRAcTOmKi/8U0tyJbrB2A3XCd7W7o6A==')

    # Create a container called 'quickstartblobs'.
    container_name ='gpistore'
    block_blob_service.create_container(container_name)

    # Set the permission so the blobs are public.
    block_blob_service.set_container_acl(container_name, public_access=PublicAccess.Container)
    
    #Verificar si la ruta existe.
    #symbols = ['^GSPC','GC=F','BG','SI=F','^DJI','GLEN.L','ADM']

    records = get_last_record_date(sql_yahoo)
    data = pd.DataFrame()

    for i in range(len(records)):
        symbol = records[i][0]
        max_date = records[i][1]

        start_date = datetime.datetime.combine(max_date, datetime.datetime.min.time()) + datetime.timedelta(days=1)
        end_date = datetime.datetime.now() - datetime.timedelta(days=1)
        if (start_date.strftime("%Y-%m-%d") > end_date.strftime("%Y-%m-%d")):
            continue

        date_now_timestamp = str(datetime.datetime.timestamp(start_date)).split('.')
        start = date_now_timestamp[0]

        date_now_timestamp = str(datetime.datetime.timestamp(end_date)).split('.')
        end = date_now_timestamp[0]

        symbol_data = get_data(symbol,start,end)
        if(symbol_data.empty):
            continue
        else:
            data = data.append(symbol_data)

    if(data.empty):
        return '{"Result":"False"}'

    file_download = data.to_csv(header=True,index=False,encoding='utf-8-sig')        
    name = 'YAHOO_FINANCE.csv'
    
    # Carga del archivo completo de WASDE al azure blob storage
    block_blob_service.create_blob_from_text(container_name,name,file_download)
    return '{"Result":"True"}'

def get_last_record_date(sql):
    server = 'grainpredictive.database.windows.net'
    database = 'gpi'
    username = 'gpi'
    password = 'Cmi@2019$A'
    driver= '{ODBC Driver 17 for SQL Server}'
    cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    cursor.execute(sql)
    return cursor.fetchall()
