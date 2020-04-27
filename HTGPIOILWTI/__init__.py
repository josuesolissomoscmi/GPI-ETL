import logging
import azure.functions as func
from azure.storage.blob import BlockBlobService, PublicAccess
from io import BytesIO
from io import StringIO
import pandas as pd
import pyodbc
import numpy as np
import datetime
import requests
from dateutil.relativedelta import relativedelta


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
        r = oil_wti()
        return func.HttpResponse(r)
    else:
        return func.HttpResponse(
             "Please pass a name on the query string or in the request body",
             status_code=400
        )

base_url_prices = 'https://markets.businessinsider.com/Ajax/Chart_GetChartData'
date_format_1 = '%Y-%m-%d'
date_format = '%Y%m%d'
sql_oil_wti = "SELECT MAX(Date) FROM [ST_MKTINSIDER].[OIL_WTI_PRICES]"

def upload_azure(values, file_name):
    # Create the BlockBlockService that is used to call the Blob service for the storage account
    block_blob_service = BlockBlobService(account_name='gpistore', account_key='zfKM5R0PuPwR0F+pPsgs5BW/AQjAxv5fwKojoP2W38II++qfT6e+axFrRAcTOmKi/8U0tyJbrB2A3XCd7W7o6A==')

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

def get_oil_wti_price(date_from, date_to):
    start_date = date_from + datetime.timedelta(days=1)
    end_date = date_to - datetime.timedelta(days=1)
    # Fetch prices from Web
    payload = {
        'instrumentType' : 'Commodity'
        ,'tkData' : '300002,6,0,333'
        ,'from' : start_date.strftime(date_format)
        ,'to' : end_date.strftime(date_format)
    }
    oil_wti_price = requests.get(base_url_prices, params=payload)
    return oil_wti_price.json()

def parse_oil_wti_price(data):
    # Convert to DataFrame
    return pd.DataFrame(data, columns=['Date', 'Close'])

def oil_wti():
    max_date = get_last_record_date(sql_oil_wti)[0][0] #.strftime(date_format)
    data = get_oil_wti_price(max_date, datetime.date.today())
    data = parse_oil_wti_price(data)
    data['actualizacion'] = datetime.datetime.now()
    if(data.empty):
        return "False"
    return upload_azure(data, 'OIL_WTI')