import logging
import azure.functions as func
from azure.storage.blob import BlockBlobService, PublicAccess
import requests
import urllib.request as ulib
from io import BytesIO
import openpyxl as xl
import xlrd
import time
import pandas as pd
import pyodbc
from bs4 import BeautifulSoup
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
        respuesta = upload_azure()
        #return func.HttpResponse(f"Hello {name}! "+respuesta)
        return func.HttpResponse(respuesta)
    else:
        return func.HttpResponse(
             "Debe ingresar el name",
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
    
    #Verificar si el archivo ya existe en WASDE
    url = "https://usda.library.cornell.edu/concern/publications/3t945q76s?locale=en"      
    if exist_fileurl(url)==False:
        return 'False'

    #Obtenemos el ultimo archivo publicado por Wasde
    req = requests.get(url)
    html = BeautifulSoup(req.content, "html.parser")
    entradas= html.find_all('tr', {'class' : 'release attributes row'})
    texto = entradas[0].find('td',{'class' : 'attribute date_uploaded'}).text
    link = entradas[0].find('td',{'class' : 'file_set'}).find('a',{'data-label' : 'latest.xls'}).attrs['href']
    daterelease = entradas[0].find('td',{'class' : 'file_set'}).find('a',{'data-label' : 'latest.xls'}).attrs['data-release-date']
    daterelease = daterelease[:10]
    
    #Verificamos si el daterealese del archivo ya fue cargado en SQL, si fue asi se termina la ejecucion
    if (exist_daterelease(daterelease)):
        return 'False'
    
    #Verificamos que el mes no exista, si existe lo eliminamos antes de cargar esta nueva version
    #Esto se hace porque pueden haber varias versiones en un mismo mes y solo necesitamos la ultima
    if (exist_monthyear(daterelease)):
        delete_wasde(daterelease)

    fname = daterelease.replace('-','')
    name = 'WASDE_'+daterelease+'.xls'
    req = ulib.urlopen(link)
    req_read = req.read()
    file_download = BytesIO(req_read)
    
    # Carga del archivo completo de WASDE al azure blob storage
    block_blob_service.create_blob_from_stream(container_name,name,file_download)

    #Extraccion de datos
    name = 'WASDE_STOCK_TO_USE.csv'
    file_download = extraer_datos(req_read,fname,daterelease)
    block_blob_service.create_blob_from_text(container_name,name,file_download)
    return 'True'

def exist_daterelease(daterelease):
    server = 'grainpredictive.database.windows.net'
    database = 'gpi'
    username = 'gpi'
    password = 'Cmi@2019$A'
    driver= '{ODBC Driver 17 for SQL Server}'
    cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    sql = "SELECT * FROM [ST_WASDE].[STOCKS_TO_USE] where [DATERELEASE ] = '"+daterelease+"'"
    n = cursor.execute(sql)
    if(n.rowcount==0):
        return False
    else:
        return True

def exist_monthyear(daterelease):
    server = 'grainpredictive.database.windows.net'
    database = 'gpi'
    username = 'gpi'
    password = 'Cmi@2019$A'
    driver= '{ODBC Driver 17 for SQL Server}'
    cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    sql = "SELECT * FROM [ST_WASDE].[STOCKS_TO_USE] where left(DATERELEASE,7)  = '"+daterelease[:7]+"'"
    n = cursor.execute(sql)
    if(n.rowcount==0):
        return False
    else:
        return True

def delete_wasde(daterelease):
    server = 'grainpredictive.database.windows.net'
    database = 'gpi'
    username = 'gpi'
    password = 'Cmi@2019$A'
    driver= '{ODBC Driver 17 for SQL Server}'
    cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    sql = "delete from [ST_WASDE].[STOCKS_TO_USE] where left(DATERELEASE,7) = '"+daterelease[:7]+"' "
    cursor.execute(sql)
    cursor.commit()

def extraer_datos(req_read,fname,daterelease):
    headers=['Origen','Archivo','Wasde','Datos','Commoditie','Medida','DateN','HarvestDate','Tipo','Grupo','Geography','Orden','Mes','Beginning stocks','Production','Imports','Domestic Feed','Domestic total','Exports','Ending stocks','Total Use','Stocks to Use']
    #Pagina 22
    datos = 'World Corn Supply and Use'
    pagina = 'Page 22'
    data=read_xls_with_sheetname(req_read,pagina) 
    processed_data=process_data_p22(data,fname,datos)                    
    df22 = pd.DataFrame(processed_data,columns=headers)

    #Pagina 23
    datos = 'World Corn Supply and Use'
    pagina = 'Page 23'
    data=read_xls_with_sheetname(req_read,pagina) 
    processed_data=process_data_p23(data,fname,datos)                    
    df23 = pd.DataFrame(processed_data,columns=headers)

    frames = [df22,df23]
    df = pd.concat(frames)
    df['actualizacion'] = datetime.datetime.now()
    df['DATERELEASE'] = daterelease


    combined_csv = df.to_csv(header=True,index=False,encoding='utf-8-sig')
    return combined_csv

def read_xls_with_sheetname(filename,sheet_name):
    '''read specific sheet by name in excel(xls) file and returns a list of lists '''
    data=[]
    book_xls = xlrd.open_workbook(file_contents=filename, formatting_info=True, ragged_rows=True)

    book_xlsx = xl.workbook.Workbook()
    sheet_names = book_xls.sheet_names()
    for sheet_index in range(len(sheet_names)):
        if sheet_name !=sheet_names[sheet_index]:
            continue
        sheet_xls = book_xls.sheet_by_name(sheet_names[sheet_index])
        if sheet_index == 0:
            sheet_xlsx = book_xlsx.active
            sheet_xlsx.title = sheet_names[sheet_index]
        else:
            sheet_xlsx = book_xlsx.create_sheet(title=sheet_names[sheet_index])
        for crange in sheet_xls.merged_cells:
            rlo, rhi, clo, chi = crange
            sheet_xlsx.merge_cells(start_row=rlo + 1, end_row=rhi,
            start_column=clo + 1, end_column=chi,)

        def _get_xlrd_cell_value(cell):
            value = cell.value
            if cell.ctype == xlrd.XL_CELL_DATE:
                datetime_tup = xlrd.xldate_as_tuple(value,0)    
                if datetime_tup[0:3] == (0, 0, 0):   # time format without date
                    value = datetime.time(*datetime_tup[3:])
                else:
                    value = datetime.datetime(*datetime_tup)
            return value

        for row in range(sheet_xls.nrows):
            new_row=[]
            try:
                for col in (_get_xlrd_cell_value(cell) for cell in sheet_xls.row_slice(row, end_colx=sheet_xls.row_len(row))):
                    new_row.append(col)
            except:
                continue
            if len(new_row) != 0:
                data.append(new_row)
  
    end = time.time()   
    return data

def process_data_p22(data,archivo,datos):
    '''Extract data from Table based on provided instructions'''
    new_data=[]    
    header_index=0
    date_index=0
    begin_stock_index=0
    production_index=0
    import_index=0
    domestic_index=0
    domestic_index_2=0
    exports_index=0
    ending_stock_index=0
    #Specify the cols index for table header(have smart detection for cols)
    for i,row in enumerate(data):
        if 'WASDE' in str(row):
            for index,cell in enumerate(row):
                if 'WASDE' in cell:
                    wasde = cell
        if 'World Corn Supply and Use' in str(row):
            for index,cell in enumerate(row):
                if 'World Corn Supply and Use' in cell:
                    commoditie = 'Corn'
        if 'Million Metric Tons' in str(row):
            for index,cell in enumerate(row):
                if 'Million Metric Tons' in cell:
                    medida = cell
        if 'Beginning\nStocks' in row:
            header_index=i
            for index,cell in enumerate(row):
                if cell.replace('/','').isdigit():
                    date_index=index
                if cell=='Beginning\nStocks':
                    begin_stock_index=index
                if cell=='Production':
                    production_index=index
                if cell=='Imports':
                    import_index=index
                if 'Domestic\nFeed' in cell:
                    domestic_index=index
                if "Domestic\nTotal" in cell:
                    domestic_index_2=index
                if cell=='Exports':
                    exports_index=index
                if cell=='Ending\nStocks':
                    ending_stock_index=index 
            break
    date=''

    #extracting data from cols
    n = 2
    for row in data[header_index:]:
        new_row=[]
        if 'Beginning\nStocks' in row:
            date=row[date_index]
            if n == 2:
                n = 1
            else:
                n = 2
            continue
        if row[date_index]=='' or '1/ Aggregate of local marketing years' in row[date_index]:
            continue
        if 'Selected Other' in row[date_index]:
            continue
        #se verifica cuando una fila contiene estos caracteres y segun el caso asigna el grupo
        if 'World' in row[date_index]:
            grupo = 'Resumen'
        if 'Major Exporters' in row[date_index]:
            grupo = 'Major Exporters'
        if 'Major Importers' in row[date_index]:
            grupo = 'Major Importers'
        new_row.append('XLS')
        new_row.append(archivo)
        new_row.append(wasde.replace(' ','').strip().upper())
        new_row.append(datos.upper()) 
        new_row.append(commoditie.upper()) 
        new_row.append(medida.upper())  
        new_row.append(n)
        new_row.append(date.split()[0])
        if len(date.split()) == 2:
            new_row.append(date.split()[1].upper())
        else:
            new_row.append('')            
        new_row.append(grupo.upper())
        new_row.append(replace_string(row[date_index].upper().strip()))
        new_row.append('2')#Orden columnas vacias porque solo se usan en pag 23
        new_row.append(obtener_mes(mid(archivo,4,2)).upper())#Mes columnas vacias porque solo se usan en pag 23        
        new_row.append(row[begin_stock_index])
        new_row.append(row[production_index])
        new_row.append(row[import_index])
        new_row.append(row[domestic_index])
        new_row.append(row[domestic_index_2])
        new_row.append(row[exports_index])
        new_row.append(row[ending_stock_index])
        total_use = float(convert_value(row[domestic_index_2])) + float(convert_value(row[exports_index]))
        if total_use == 0:
            stock_to_use = 0
        else:            
            stock_to_use = (float(convert_value(row[ending_stock_index])) / total_use) * 100
        new_row.append(total_use)
        new_row.append(stock_to_use)        
        new_data.append(new_row)
    return new_data

def process_data_p23(data,archivo,datos):
    '''Extract data from Table based on provided instructions'''
    new_data=[]
    header_index=0
    date_index=0
    report_date_index=0   
    begin_stock_index=0
    production_index=0
    import_index=0
    domestic_index=0
    domestic_index_2=0
    exports_index=0
    ending_stock_index=0
    #Specify the cols index for table header(have smart detection for cols)
    for i,row in enumerate(data):
        if 'WASDE' in str(row):
            for index,cell in enumerate(row):
                if 'WASDE' in cell:
                    wasde = cell
        if 'World Corn Supply and Use' in str(row):
            for index,cell in enumerate(row):
                if 'World Corn Supply and Use' in cell:
                    commoditie = 'Corn'
        if 'Million Metric Tons' in str(row):
            for index,cell in enumerate(row):
                if 'Million Metric Tons' in cell:
                    medida = cell                    
        if 'Beginning\nStocks' in row:
            header_index=i
            for index,cell in enumerate(row):
                if cell=='':
                    continue
                if  cell.split()[0].replace('/','').isdigit():
                    date_index=index
                if cell=='Beginning\nStocks':
                    begin_stock_index=index
                    report_date_index = begin_stock_index-1
                if cell=='Production':
                    production_index=index
                if cell=='Imports':
                    import_index=index
                if 'Domestic\nFeed' in cell:
                    domestic_index=index
                if "Domestic\nTotal" in cell:
                    domestic_index_2=index
                if cell=='Exports':
                    exports_index=index
                if cell=='Ending\nStocks':
                    ending_stock_index=index 
            break
    date=''
    #extracting data from cols
    geoant = ""
    for index,row in enumerate(data[header_index:]):
        new_row=[]
        if 'Beginning\nStocks' in row:
            date=row[date_index]
            continue
        if (row[report_date_index]=='' and row[date_index] =='') or '1/ Aggregate of local marketing years' in row[date_index]:
            continue
        if 'Selected Other' in row[date_index]:
            continue
        #se verifica cuando una fila contiene estos caracteres y segun el caso asigna el grupo
        if 'World' in row[date_index]:
            grupo = 'Resumen'
        if 'Major Exporters' in row[date_index]:
            grupo = 'Major Exporters'
        if 'Major Importers' in row[date_index]:
            grupo = 'Major Importers'                    
        
        new_row.append('XLS')        
        new_row.append(archivo)
        new_row.append(wasde.replace(' ','').strip().upper())        
        new_row.append(datos.upper()) 
        new_row.append(commoditie.upper()) 
        new_row.append(medida.upper())                         
        new_row.append('3')
        new_row.append(date.split()[0])
        if len(date.split()) == 2:
            new_row.append(date.split()[1].upper())
        else:
            new_row.append('')
        new_row.append(grupo.upper())                    
        if row[date_index].strip() == '' and row[report_date_index].strip() != '':
            row[date_index] = data[header_index:][index - 1][date_index]
        geo = replace_string(row[date_index].upper().strip())
        if geo == geoant:
            orden = 2
        else:
            orden = 1
        new_row.append(geo)  #add geographic
        new_row.append(orden)
        new_row.append(row[report_date_index].upper())
        new_row.append(convert_value(row[begin_stock_index]))
        new_row.append(convert_value(row[production_index]))
        new_row.append(convert_value(row[import_index]))
        new_row.append(convert_value(row[domestic_index]))
        new_row.append(convert_value(row[domestic_index_2]))
        new_row.append(convert_value(row[exports_index]))
        new_row.append(convert_value(row[ending_stock_index]))
        total_use = float(convert_value(row[domestic_index_2])) + float(convert_value(row[exports_index]))
        if total_use == 0:
            stock_to_use = 0
        else:            
            stock_to_use = (float(convert_value(row[ending_stock_index])) / total_use) * 100
        new_row.append(total_use)
        new_row.append(stock_to_use)        
        new_data.append(new_row)
        geoant = geo
    return new_data

def replace_string(cadena):
    for ch in ['1/','2/','3/','4/','5/','6/','7/','8/','9/','10/']:
        if ch in cadena:
            cadena=cadena.replace(ch,"")
    return cadena 

def convert_value(valor):
    if valor == '' or valor == 'NA':
        valor = 0
    return valor 

def mid(s, offset, amount):
    return s[offset:offset+amount]

def left(s, amount):
    return s[:amount]

def right(s, amount):
    return s[-amount:]

def obtener_mes(month):
    month_dictionary = {'01':'jan','02':'feb','03':'mar','04':'apr','05':'may','06':'jun',
    '07':'jul','08':'aug','09':'sep','10':'oct','11':'nov','12':'dec'}
    return month_dictionary.get(month)

def exist_fileurl(url):
    request = requests.get(url)
    if request.status_code == 200:
        return True
    else:
        return False
