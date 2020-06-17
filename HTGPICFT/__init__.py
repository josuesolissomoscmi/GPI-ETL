import logging
import azure.functions as func
from azure.storage.blob import BlockBlobService, PublicAccess
import requests
from urllib.request import urlopen
from io import BytesIO
from io import StringIO
import time
import datetime
import pandas as pd
import pyodbc
from zipfile import ZipFile
#import zipfile


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

def upload_azure():
    # Create the BlockBlockService that is used to call the Blob service for the storage account
    block_blob_service = BlockBlobService(account_name='gpistore', account_key='zfKM5R0PuPwR0F+pPsgs5BW/AQjAxv5fwKojoP2W38II++qfT6e+axFrRAcTOmKi/8U0tyJbrB2A3XCd7W7o6A==')

    # Create a container called 'quickstartblobs'.
    container_name ='gpistore'
    block_blob_service.create_container(container_name)

    # Set the permission so the blobs are public.
    block_blob_service.set_container_acl(container_name, public_access=PublicAccess.Container)
    
    #revisar la ultima fecha cargada
    last_date = get_last_record_date()
    year_actual = time.strftime("%Y")
    year_last_load = int(last_date[0][0].strftime("%Y"))
    week_last_load = int(last_date[0][0].strftime("%U"))
    date_last_load = last_date[0][0].strftime('%Y-%m-%d')
    week_last_year = datetime.date(int(year_last_load),12,31).strftime("%U")

    if(week_last_load < 52):
        year = year_last_load

    if(week_last_load == 52):
        year = year_last_load+1
    
    #Verificar si el archivo existe
    #Se actualiza todos los martes
    url = 'https://www.cftc.gov/files/dea/history/com_disagg_xls_' + str(year) + '.zip'      
    if exist_fileurl(url)==False:
        return '{"Result":"False"}'
    
    name = 'FONDOS_CFTC.csv'
    file_download = download_inmemory(url,year,date_last_load)

    block_blob_service.create_blob_from_text(container_name,name,file_download)
    return '{"Result":"True"}'   

def get_last_record_date():
    server = 'grainpredictive.database.windows.net'
    database = 'gpi'
    username = 'gpi'
    password = 'Cmi@2019$A'
    driver= '{ODBC Driver 17 for SQL Server}'
    cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
    sql = 'SELECT max(Report_Date_as_MM_DD_YYYY) as date_last FROM [ST_CFTC].[DISAGGRETADED_FUTURES_OPTIONS]'
    cursor = cnxn.cursor()
    cursor.execute(sql)
    dates = []
    for row in cursor:
        date_row = []
        date = row[0]
        date_row.append(date)                
        dates.append(date_row)
    return dates

def download_inmemory(url,year,date_last_load):
    #Todos los martes se publica nueva informacion nueva
    url = urlopen(url)
    zf = ZipFile(BytesIO(url.read()))
    for item in zf.namelist():
        print("File in zip: "+  item)
    match = [s for s in zf.namelist() if ".xls" in s][0]
    xlsfile = BytesIO(zf.read(match))
    df = pd.read_excel(xlsfile, sheet_name='XLS')
    df['año'] = year
    df['calculo'] = df['M_Money_Positions_Long_ALL'] - df['M_Money_Positions_Short_ALL']     
    df.fillna(0, inplace=True)
    df = df[df['Report_Date_as_MM_DD_YYYY']>date_last_load]
    df['actualizacion'] = datetime.datetime.now()
    csv_inmemory = df.to_csv(header=True,index=False,encoding='utf-8')
    return csv_inmemory

def exist_fileurl(url):
    request = requests.get(url)
    if request.status_code == 200:
        return True
    else:
        return False