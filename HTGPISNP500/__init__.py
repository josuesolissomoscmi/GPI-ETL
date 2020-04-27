import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient, PublicAccess
from io import BytesIO
from io import StringIO
import pandas as pd
import pyodbc
import numpy as np
import requests
from dateutil.relativedelta import relativedelta
from datetime import date
from datetime import timedelta
import datetime


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
        r = snp_500()
        return func.HttpResponse(r)
    else:
        return func.HttpResponse(
             "Please pass a name on the query string or in the request body",
             status_code=400
        )

base_url_prices = 'https://query1.finance.yahoo.com/v7/finance/spark'
date_format_1 = '%Y-%m-%d'
date_format = '%Y%m%d'
sql_snp500 = "SELECT MAX(Date) FROM [ST].[YAHOOFIN_SNP_500]"


def upload_azure(values, file_name):
    # Create the BlockBlockService that is used to call the Blob service for the storage account
    block_blob_service = BlobServiceClient(account_url='https://gpistore.blob.core.windows.net', credential='zfKM5R0PuPwR0F+pPsgs5BW/AQjAxv5fwKojoP2W38II++qfT6e+axFrRAcTOmKi/8U0tyJbrB2A3XCd7W7o6A==')

    # Create a container called 'quickstartblobs'.
    container_name ='gpistore'
    block_blob_service.create_container(container_name)

    # Set the permission so the blobs are public.
    block_blob_service.set_container_acl(container_name, public_access=PublicAccess.Container)    

    #Extraccion de datos
    name = file_name+'.csv'
    values_csv = values.to_csv(header=True,index=False,encoding='utf-8-sig')
    block_blob_service.create_blob_from_text(container_name,name,values_csv)
    return 'True'

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

def get_snp500_price(rango):
    # Fetch prices from Web
    payload = {
        'symbols' : '^GSPC'
        ,'range' : rango
        ,'interval' : '1d'
    }
    snp500_price = requests.get(base_url_prices, params=payload)
    return snp500_price.json()

def parse_snp500_price(data,last_date):
    start_date = last_date + datetime.timedelta(days=1)
    end_date = datetime.datetime.now() - datetime.timedelta(days=1)
    prices = data["spark"]["result"][0]["response"][0]["indicators"]["quote"][0]["close"]
    timestamp = data["spark"]["result"][0]["response"][0]["timestamp"]
    snp500_price = pd.DataFrame(columns=['Date','Close'])
    snp500_price['Date'] = timestamp
    snp500_price['Date'] = pd.to_datetime(snp500_price['Date'],unit='s')
    snp500_price['Close'] = prices
    snp500_price = snp500_price[snp500_price['Date']>start_date] 
    snp500_price = snp500_price[snp500_price['Date']<=end_date]    
    return snp500_price

def snp_500():
    max_date = get_last_record_date(sql_snp500)[0][0] 
    #se usan 60 dias por si la cadena falla en algun punto y pasan varios dias sin que se actualice para tener un rango amplio
    #mas adelante se filtra para que solo actualice lo nuevo segun la ultima fecha actualizada
    data = get_snp500_price('60d')
    data = parse_snp500_price(data,max_date)
    data['actualizacion'] = datetime.datetime.now()
    if(data.empty):
        return "False"
    return upload_azure(data, 'SNP_500')