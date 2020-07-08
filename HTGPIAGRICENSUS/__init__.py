import logging
import azure.functions as func
from azure.storage.blob import BlockBlobService, PublicAccess
from io import BytesIO
from io import StringIO
import pandas as pd
import pyodbc
import requests
import datetime
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
        r = agricensus()
        return func.HttpResponse(r)
    else:
        return func.HttpResponse(
             "Please pass a name on the query string or in the request body",
             status_code=400
        )

base_url_history = 'https://www.agricensus.com/feed/?format=CSV&historical'
username = 'raguilar@mfgrains.com'
password = 'cmi_Alimentos1'
sql_inflation = "select max(date) from [ST_AGRICEN].[AGRICENSUS_FOB]"

def upload_azure(values, file_name):
    # Create the BlockBlockService that is used to call the Blob service for the storage account
    block_blob_service = BlockBlobService(account_name='gpistore', account_key='zfKM5R0PuPwR0F+pPsgs5BW/AQjAxv5fwKojoP2W38II++qfT6e+axFrRAcTOmKi/8U0tyJbrB2A3XCd7W7o6A==')

    # Create a container called 'quickstartblobs'.
    container_name ='gpistore'
    block_blob_service.create_container(container_name)

    # Set the permission so the blobs are public.
    block_blob_service.set_container_acl(container_name, public_access=PublicAccess.Container)    

    #Extraccion de datos
    name = file_name + '.csv'
    values_csv = values.to_csv(header=True,index=False,encoding='utf-8-sig')
    block_blob_service.create_blob_from_text(container_name,name,values_csv)
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

def get_agricensus_history():
    # Read feed from Web API
    raw_agricensus_history = requests.get(base_url_history, auth=(username, password))
    raw_agricensus_history = StringIO(raw_agricensus_history.content.decode('utf-8'))
    return raw_agricensus_history

def parse_agricensus_history(data):
    # Convert to DataFrame
    agricensus_hisotry = pd.read_csv(data, sep=",")
    # Drop empty column name
    agricensus_hisotry = agricensus_hisotry.drop(axis='columns', columns='name')
    return agricensus_hisotry

def filter_agricensus_history(data, date):
    # Filter by date
    return data[data['date'] > date]

def agricensus():
    max_date = get_last_record_date(sql_inflation)[0][0].strftime('%Y-%m-%d')
    data = get_agricensus_history()
    data = parse_agricensus_history(data)
    data = filter_agricensus_history(data, max_date)
    data['actualizacion'] = datetime.datetime.now().strftime('%Y-%m-%d')

    if(data.empty):
        return '{"Result":"False"}'
    return upload_azure(data, 'AGRICENSUS')
