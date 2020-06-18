#fjsolis - TEST
import logging
import azure.functions as func
from azure.storage.blob import BlockBlobService, PublicAccess
import requests
import urllib.request as ulib
from io import BytesIO
import time
import datetime
import pandas as pd
import pyodbc


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

def exist_fileurl(url):
    request = requests.get(url)
    if request.status_code == 200:
        return True
    else:
        return False

def upload_azure():
    # Create the BlockBlockService that is used to call the Blob service for the storage account
    block_blob_service = BlockBlobService(account_name='gpistore', account_key='zfKM5R0PuPwR0F+pPsgs5BW/AQjAxv5fwKojoP2W38II++qfT6e+axFrRAcTOmKi/8U0tyJbrB2A3XCd7W7o6A==')

    # Create a container called 'quickstartblobs'.
    container_name ='gpistore'
    block_blob_service.create_container(container_name)

    # Set the permission so the blobs are public.
    block_blob_service.set_container_acl(container_name, public_access=PublicAccess.Container)
    
    #Verificar si el archivo ya existe en WASDE
    url = 'https://www.cpc.ncep.noaa.gov/data/indices/wksst8110.for'      
    if exist_fileurl(url)==False:
        return '{"Result":"False"}'

    #se elimina la informacion del mes actual en caso ya se haya cargado una vez
    enso_year = year = time.strftime("%Y")
    #delete_enso(enso_year)
    
    name = 'ENSO.csv'
    file_download = download_inmemory(url,enso_year)

    block_blob_service.create_blob_from_text(container_name,name,file_download)
    return '{"Result":"True"}'   

def download_inmemory(url,enso_year):
    #Todos los miercoles se publica nueva informacion de enso
    req = ulib.urlopen(url)
    req_read = req.read()
    file_download = BytesIO(req_read)
    
    headers = ['WEEK','DEL','SST_NINO12','SSTA_NINO12','DEL','SST_NINO3','SSTA_NINO3','DEL','SST_NINO34','SSTA_NINO34','DEL','SST_NINO4','SSTA_NINO4']
    month_dictionary = {'JAN':'01','FEB':'02','MAR':'03','APR':'04','MAY':'05','JUN':'06','JUL':'07','AUG':'08','SEP':'09','OCT':'10','NOV':'11','DEC':'12'}
    #Convertir el archivo txt de ancho fijo a columnas indicando la longitud de cada columna
    archivo = pd.read_fwf(file_download,widths=[10,5,4,4,5,4,4,5,4,4,5,4,4], skiprows=4, header=None)
    df = pd.DataFrame(archivo.values,columns=headers)
    #se elimina las columnas con nombre DEL, ya que no tienen informacion
    df = df.drop(['DEL'], axis=1)
    df['DAY']=df['WEEK'].str[:2]
    df['MONTH']= df['WEEK'].str[2:5].map(month_dictionary)
    df['YEAR']=df['WEEK'].str[-4:]
    df['DATE']=df['YEAR']+'-'+df['MONTH']+'-'+df['DAY']
    #df = df.loc[df['YEAR']=='2019']
    df = df[df['YEAR']==enso_year]
    df['actualizacion'] = datetime.datetime.now()
    csv_inmemory = df.to_csv(header=True,index=False,encoding='utf-8-sig')
    return csv_inmemory
