import logging
import azure.functions as func
from azure.storage.blob import BlockBlobService, PublicAccess
from io import BytesIO
from io import StringIO
import pandas as pd
import pyodbc
import numpy as np
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
        r = inflation()
        return func.HttpResponse(r)
    else:
        return func.HttpResponse(
             "Please pass a name on the query string or in the request body",
             status_code=400
        )

base_url = 'https://www.usinflationcalculator.com/inflation/consumer-price-index-and-annual-percent-changes-from-1913-to-2008/'
sql_inflation = "select MAX(date) from [ST_USINFLAT].[INFLATION_US]"

months_dict = {
    'Jan' : 1,
    'Feb' : 2,
    'Mar' : 3,
    'Apr' : 4,
    'May' : 5,
    'June' : 6,
    'July' : 7,
    'Aug' : 8,
    'Sep' : 9, 
    'Oct' : 10,
    'Nov' : 11,
    'Dec' : 12
}

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

def get_inflation():
    # HTML Table from Web to DataFrame
    raw_inflation = pd.read_html(base_url, header=1, skiprows=0)[0]
    # Replace empty values
    raw_inflation = raw_inflation.replace('–', np.nan)
    # Drop last 3 columns (aggregation columns)
    raw_inflation = raw_inflation.iloc[:, :-3]
    return raw_inflation

def parse_inflation(df):
    # Transpose Months columns to rows
    inflation = df.melt(id_vars=["Year"], var_name="Month", value_name="Inflation")
    # Replace empty values
    inflation = inflation.replace('', np.nan)
    # Remove empty values
    inflation = inflation.dropna(subset=["Inflation"])
    # Change month name to number
    inflation['Month'] = inflation['Month'].apply(lambda x: months_dict.get(x))
    return inflation

def filter_inflation(df, date):
    inflation_filtered = df[df['date'] > date]
    return inflation_filtered

def inflation():
    date = get_last_record_date(sql_inflation)[0][0].strftime('%Y-%m-%d')
    data = get_inflation()
    data = parse_inflation(data)
    data['Year'] = data['Year'].astype(str)
    data['Month'] = data['Month'].astype(str)
    data['date'] = data['Year']+'-'+data['Month']+'-01'
    data['date'] = pd.to_datetime(data['date'])
    data['date']  = data['date'].map(lambda x: x + relativedelta(day=31))
    data = filter_inflation(data, date)
    data['actualizacion'] = datetime.datetime.now()
    if(data.empty):
        return "False"
    return upload_azure(data, 'US_INFLATION')