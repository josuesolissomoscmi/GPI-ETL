"""
Created on Thu Nov 12 16:18:07 2020

@author: javier.fong
"""

import logging
import pandas as pd
import requests as rq
import json
import datetime
import time
import re
import io
import sys
import joblib
import numpy as np
import geopy 
import pyodbc 
from sklearn.ensemble import RandomForestClassifier
from geopy.distance import geodesic
from azure.storage.blob import BlockBlobService, PublicAccess
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    lat = req.params.get('lat')
    lon = req.params.get('lon')

    coordinates = (lat, lon)
    
    if coordinates:
        response = NEX_MAIN(lat, lon)
        return func.HttpResponse(response)
    else:
        return func.HttpResponse(
             "Pass the coordinates in the query string or in the request body for a personalized response.",
             status_code=200
        )

output = pd.DataFrame()

def find_places(rst_cd, lat, lon, category, pagetoken):
    api_url_base = 'https://maps.googleapis.com/maps/api/place/nearbysearch/json?'
    api_key = 'AIzaSyBkWP2Oj91ITxAPfHUHWRM4x2XkIxqqvf8'
    
    global output
    if pagetoken is None:
        api_url_request = api_url_base + 'location=' + str(lat) + ',' + str(lon) + '&radius=300' + '&type=' + category + '&key=' + api_key
    else:
        api_url_request = api_url_base + 'location=' + str(lat) + ',' + str(lon) + '&radius=300' + '&type=' + category + '&key=' + api_key + pagetoken
    #print(api_url_request)
    response = rq.get(api_url_request)
    res = json.loads(response.text)
    #print(res)
    for result in res["results"]:
        #print(category)
        info = {}
        info['rst_cd'] = rst_cd
        info['place_ltt'] = lat
        info['place_lgt'] = lon
        info['poi_id'] = result['place_id']
        info['poi_name'] = result["name"]
        info['poi_type'] = category.upper()
        info['poi_ltt'] = result["geometry"]["location"]["lat"]
        info['poi_lgt'] = result["geometry"]["location"]["lng"]
        #print(info)
        output = output.append(info, ignore_index=True)
    pagetoken = res.get("next_page_token",None)
    return pagetoken


def download_azure():
    DEST_FILE='ip_hn_pollolandia_model.sav'
    # Create the BlockBlockService that is used to call the Blob service for the storage account
    block_blob_service = BlockBlobService(account_name='gpistore', account_key='zfKM5R0PuPwR0F+pPsgs5BW/AQjAxv5fwKojoP2W38II++qfT6e+axFrRAcTOmKi/8U0tyJbrB2A3XCd7W7o6A==')

    # Create a container called 'quickstartblobs'.
    container_name ='cmia'
    #print(DEST_FILE)

    stream = io.BytesIO()
    block_blob_service.get_blob_to_stream(container_name,DEST_FILE,stream=stream,max_connections=2)
    #stream.close()

    return stream

def close_points(CNTRY, CTGRY, LTT, LNG):
    server = 'cmiazsrvml03.database.windows.net'
    database = 'IDN_DB'
    username = 'cmia_etl'
    password = '(Mi@.3Tl'   
    driver= '{ODBC Driver 17 for SQL Server}'
    
    conn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) 

    if CTGRY == 'Casa_Del_Pollo':
        CTGRY = 'CASA DEL POLLO'
    else:
        CTGRY = 'POLLOLANDIA'
    
    query = """
        SELECT 
        	ROW_NUMBER() OVER (ORDER BY mdist ASC) as row_index
        	, * 
        FROM (
        	SELECT TOP 3 
        		POS_NM
        		, round(mdist, 0) mdist
        	FROM (
        		SELECT *, geography::Point("""+str(LTT)+""","""+str(LNG)+""", 4326).STDistance(geography::Point(nex_geo.LTT , nex_geo.LGT, 4326)) as mdist
        		FROM DIM.CMIA_IP_NEX_GEO_POINTS nex_geo
        		WHERE 
        		--CNTRY_NM = '"""+CNTRY+"""'
        		CTGRY_NM = '"""+CTGRY+"""'
        	) SQ
        	ORDER BY mdist
        ) CP"""
    close_points = pd.read_sql(query, conn)
    res = [] 
    for index, row in close_points.iterrows():
        row_res = {}
        row_res['POS_RANK'] = row['row_index']
        row_res['POS_NM'] = row['POS_NM']
        row_res['POS_DIST'] = row['mdist']
        res.append(row_res)
    return(res)

def NEX_MAIN(lat, lon):
    categories = ['atm','bank','bus_station','cafe','church','convenience_store','department_store','electronics_store','hospital','local_government_office','establishment','parking','police','restaurant','school','shopping_mall','store','university']


    #output = pd.DataFrame()
    global output

    time_stamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    rst_cd = 'NEW_POINT_' + time_stamp
    ltt_num = lat
    lgt_num = lon
    for category in categories: 
        pagetoken = None
        #print(rst_cd, ltt_num, lgt_num, category, pagetoken)
        while True: 
            pagetoken = find_places(rst_cd, ltt_num, lgt_num, category, pagetoken)
            time.sleep(3)
            if not pagetoken:
                break
    
    #print(output)
    #output.to_csv(path + '\\point_' + time_stamp + '_pois_download.csv')

    #output = pd.read_csv(path + '\\point_' + time_stamp + '_pois_download.csv')

    output['poi_lgt'] = round(output['poi_lgt'], 5)
    output['poi_ltt'] = round(output['poi_ltt'], 5)

    output['poi_name'] = [poi_name.upper() for poi_name in output['poi_name']]
    output['poi_type'] = [poi_type.upper() for poi_type in output['poi_type']]

    output['poi_name'] = [re.sub('Á', 'A', poi_name) for poi_name in output['poi_name']]
    output['poi_name'] = [re.sub('É', 'E', poi_name) for poi_name in output['poi_name']]
    output['poi_name'] = [re.sub('Í', 'I', poi_name) for poi_name in output['poi_name']]
    output['poi_name'] = [re.sub('Ó', 'O', poi_name) for poi_name in output['poi_name']]
    output['poi_name'] = [re.sub('Ú', 'U', poi_name) for poi_name in output['poi_name']]
    output['poi_name'] = [re.sub("'", '', poi_name) for poi_name in output['poi_name']]
    output['poi_name'] = [re.sub('"', '', poi_name) for poi_name in output['poi_name']]
    output['poi_name'] = [re.sub(',', '', poi_name) for poi_name in output['poi_name']]
    output['poi_name'] = [re.sub('\.', '', poi_name) for poi_name in output['poi_name']]

    output['poi_category'] = None

    regex = r'(?:RESTAURANT|MEAL_DELIVERY)'
    output.loc[[re.search(regex, poi_type) is not None for poi_type in output['poi_type']], 'poi_category'] = 'OTROS RESTAURANTE'
    regex = r'\b(?:RESTAURANTE?)\b'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'OTROS RESTAURANTE'

    regex = r'(?:CALZADO|ZAPATERIA|ALMACEN|COMERCIAL|LA BODEGONA|DISTRIBUIDORA)'
    output.loc[[re.search(regex, poi_type) is not None for poi_type in output['poi_name']], 'poi_category'] = 'ALMACEN'
    regex = r'(?:CLOTHING_STORE|DEPARTMENT_STORE)'
    output.loc[[re.search(regex, poi_type) is not None for poi_type in output['poi_type']], 'poi_category'] = 'ALMACEN'

    regex = r'(?:BARBER|BEAUTY|BELLEZA|PELUQUER|NAILS)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'BARBERÍA/BELLEZA'

    regex = r'(?:IGLESIA|TEMPLO|PARROQUIA|ASAMBLEA DE DIOS|TESTIGOS?.+JEHOV)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'IGLESIA'
            
    regex = r'(?:HOSPITAL|IGSS|IGGS|CENTRO MEDICO|APROFAM|CENTRO DE SALUD|PUESTO DE SALUD|SANATORIO|HEALTH CENTER|EMERGENCIA|CIRUGIA|PEDIATRICO|SANATORIUM)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'HOSPITAL'
        
    regex = r'(?:CLINIC|MEDI|OPTIC|ODONTO|LABORATORIO|DR )'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'CLINICA DE SALUD'

    regex = r'(?:BUS |TRANSMETRO|TRANSURBANO|BUS STATION|AUTOBUSES|TERMINAL|ESTACION DE BUS|PARADA DE |BUSES|LITEGUA)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'PARADA DE BUS'

    regex = r'(?:MANUALIDADES|LIBRERIA|PAPELERIA)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'LIBRERIA'

    regex = r'(?:PARQUEO|ESTACIONAMIENTO|PARKING)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'PARQUEO'

    regex = r'(?:USAC|UNIVERSIDAD|UMG|FACULTAD|UPANA)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'UNIVERSIDAD'

    regex = r'(?:HOTEL|PENSION|HOSPEDAJE)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'HOTEL'

    regex = r'(?:GASOLINERA|GASOLINA|ESTACION DE SERVICIO|\bGAS\b)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'GASOLINERA'

    regex = r'(?:MINI MUNI|MINISTRY PUBLIC|MUNICIPALITY|SECRETARIA DE|MUNICIPALIDAD|JUZGADO|TRIBUNAL|INACIF|RENAP|MAYCOM|ORGANISMO JUDICIAL|MINISTERIO PUBLICO|MUNICIPALIDAD)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'OFICINAS GUBERNAMENTALES'

    regex = r'(?:POLICE|PNC|COMISARIA|POLICIA)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'ESTACION POLICIAL'

    regex = '(?:ESTACION DE BOMBEROS)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'ESTACION DE BOMBEROS'

    regex = r'(?:SUPER 24|SUPER PUMA|CONVENIENCIA|GASO MARKET|TIENDAS? MASS)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'TIENDA DE CONVENIENCIA'

    regex = r'(?:SUPER 24|SUPER PUMA|CONVENIENCIA|GASO MARKET|TIENDAS? MASS)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'TIENDA DE CONVENIENCIA'

    regex = r'(?:CARNICERIA|POLLERIA|CASA DEL POLLO)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'VENTA DE CARNES'

    regex = r'(?:BANRURAL|CREDOMATIC|AGROMERCANTIL|BANCO|\bBAC\b|\bBAM\b|BANTRAB|GYT|G&T|COOPERATIVA|WESTERN UNION)'
    output.loc[[re.search(regex, poi_type) is not None for poi_type in output['poi_name']], 'poi_category'] = 'BANCO'
    regex = r'(?:BANK)'
    output.loc[[re.search(regex, poi_type) is not None for poi_type in output['poi_type']], 'poi_category'] = 'BANCO'

    regex = r'\b(?:ATM|CAJERO)\b'
    output.loc[[re.search(regex, poi_type) is not None for poi_type in output['poi_name']], 'poi_category'] = 'CAJERO'
    regex = r'(?:ATM)'
    output.loc[[re.search(regex, poi_type) is not None for poi_type in output['poi_type']], 'poi_category'] = 'CAJERO'

    regex = r'(?:PARQUE|CANCHA|ESTADIO|CAMPOS)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'PARQUE'

    regex = r'(?:COLEGIO|INSTITUTO|LICEO|SEMINARIO|CENTRO EDUCATIVO|ESCUELA|CENTRO DE ESTUDIOS|\bINEB|\bINTECAP)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'COLEGIO'

    regex = r'(?:COLEGIO|ACADEMIA|ESCUELA)(?:BAILE|FUTBOL|MUSICA)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'ACADEMIA'

    regex = r'(?:ALMACENES TROPIGAS|AGENCIAS? WAY|GALLO MAS GALLO|ELEKTRA|ELECKTRA|CURACAO|ALMACENES JAPON|LA CHAPINITA|TECNO FACIL|TIENDAS MAX|RADIOSHACK|INTELAF|ELECTRONICA PANAMERICANA|PANAMERICAN ELECTRONIC|ABM|EL DUENDE)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'ALMACEN DE ELECTRODOMESTICOS'

    regex = r'(?:POLLO CAMPERO|BURGER KING|MCDONALDS|PIZZA HUT|DOMINOS PIZZA|PAPA JOHNS|TACO BELL|HAMBURGUESAS BERLIN|HAMBURGUESAS DEL PUENTE|POLLO BUJO|LITTLE CAESARS|LITTLE CEASERS)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'CADENA QSR'

    regex = r'(?:AL MACARONE|LOS GAUCHITOS|PINULITO|POLLOLANDIA|POLLO LANDIA|QUEDELY|GUATEBURGER|GUATE BURGER|POLLO GRANJERO|POLLO EXPRESS)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'CADENA QSR POPULAR'

    regex = r'(?:ANTOJITOS|CAFETERIA|ASADOS|CHURRASCOS|HAMBURGUESAS|BURGER|PUPUSERIA|PUPUSAS|CARNITA|CEVICHE|SEVICHE|CHICHARRONE|TACOS? |SHUCO|HOT DOG|TAQUERIA|COMEDOR|ALMUERZOS|LICUADOS|CHURRASQU|CAFETERIA)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'RESTAURANTE POPULAR'

    regex = r'(?:\bCAFE\b|EL CAFETALITO|BARISTA|STARBUCKS|COFFEE|COFFE|KAPE|PATSY|BAGEL|CAFFE|DUNKIN DONUTS|AMERICAN DOU)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'COFFEE SHOP'

    regex = r'(?:LA NEVERIA|HELADERIA|HELADO|POPS)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'HELADERIA'

    regex = r'(?:CANELLA|YAMAHA|HONDA|MOTOS |MOTOCICLETAS|MASESA|TIENDA UMA|MOVESA|ITALIKA|HAOJUE|BAJAJ|SUZUKI)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'TIENDA MOTOCICLETAS'

    regex = r'(?:AGENCIA TIGO|\bTIGO\b|\bCLARO\b|AGENCIA CLARO|MOVISTAR)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'AGENCIA TELEFONIA'

    regex = r'(?:SUPERMERCADO|SUPER MERCADO|WALMART|DESPENSA FAMILIAR|LA TORRE|PAIZ|MAXI BODEGA|SUPER DEL BARRIO|LA BARATA)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'SUPERMERCADO'

    regex = r'(?:\bMERCADO )'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'MERCADO CANTONAL'

    regex = r'(?:TIENDA|ABARROTERIA|ABARROTES|DEPOSITO|MAYOREO|TDA)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'TIENDA DE BARRIO'

    regex = r'(?:TORTILLERIA|TORTILLA)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'TORTILLERIA'

    regex = r'(?:PANADERIA|PANIFICADORA|PASTELERIA|\bPAN |\bPASTELES)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'PANADERIA'

    regex = r'\b(?:CENTRO COMERCIAL|PLAZA|PASEO|CC|C\.C\.|C\. C\.|MALL|GRAN VIA|GRAN CENTRO|GALERIAS|MEGA CENTRO|METRO NORTE|PRADERA)\b'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'CENTRO COMERCIAL'

    regex = r'\b(?:TALLER|MOTO|MOTOPARTES|MOTOSERVICIOS|MOTOREPUESTOS|FIGUEPARTES|AQUARONI|KARS|HUESERA|REPUESTOS|AUTO\s?REPUESTOS|AUTO\s?PIEZAS|AUTO\s?PARTES|TALLER DE |ACEITERA|MOTOCENTRO|AUTOS|LUBRI|LLANTA|BATERIAS|COPHER)\b'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'REPUESTOS VEHICULOS'

    regex = r'(?:FERRE|CELASA|CONSTRUCC|FERRO|FERETERIA)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'FERRETERIA'

    regex = r'(?:AGROVET|AGRO|AGRIC)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'AGROPECUARIA'

    regex = r'(?:FARMACIA|CRUZ VERDE|PHARMACY|FARMAZUL)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'FARMACIA'

    regex = r'(?:LOCAL_GOVERNMENT_OFFICE)'
    output.loc[[re.search(regex, poi_type) is not None for poi_type in output['poi_type']], 'poi_category'] = 'OFICINAS GUBERNAMENTALES'

    regex = r'(?:CHURCH)'
    output.loc[[re.search(regex, poi_type) is not None for poi_type in output['poi_type']], 'poi_category'] = 'IGLESIA'

    output.loc[[poi_category is None for poi_category in output['poi_category']], 'poi_category'] = 'OTROS SIN CLASIFICACION'

    # FINAL SETTING
    output.drop_duplicates(inplace=True)
    #print(output[['poi_name','poi_category', 'poi_lgt', 'poi_ltt']].sort_values(by=['poi_name']))

    pois_distance = []
    for index, poi in output.iterrows(): 
        pois_distance.append(geodesic((poi['place_ltt'], poi['place_lgt']), (poi['poi_ltt'],poi['poi_lgt'])).meters)

    output['distance_mtrs'] = pois_distance

    categories = ['BANCO',  'FARMACIA',  'COFFEE SHOP',  'RESTAURANTE POPULAR',  'IGLESIA',  'CLINICA DE SALUD',  'OFICINAS GUBERNAMENTALES',  'OTROS SIN CLASIFICACION',  'ALMACEN',  'TIENDA DE BARRIO',  'OTROS RESTAURANTE',  'COLEGIO',  'REPUESTOS VEHICULOS',  'SUPERMERCADO',  'FERRETERIA',  'TIENDA MOTOCICLETAS',  'PANADERIA',  'CADENA QSR POPULAR',  'HOSPITAL',  'AGROPECUARIA',  'AGENCIA TELEFONIA',  'PARQUE',  'MERCADO CANTONAL',  'HOTEL', 'CENTRO COMERCIAL',  'HELADERIA',  'VENTA DE CARNES',  'CAJERO', 'ALMACEN DE ELECTRODOMESTICOS',  'BARBERÍA/BELLEZA',  'CADENA QSR', 'UNIVERSIDAD',  'TORTILLERIA',  'ESTACION DE BOMBEROS',  'ESTACION POLICIAL', 'LIBRERIA',  'PARQUEO',  'GASOLINERA',  'PARADA DE BUS']
    restaurants = output.rst_cd.unique()

    forecast_data = []

    for rst in restaurants: 
        rst_forecast_data = {}
        
        rst_data = output[output.rst_cd.eq(rst)]
        
        rst_forecast_data['RST_CD'] = rst
        rst_forecast_data['GEO_POI_300M_CNT'] = len(rst_data.index)
        rst_forecast_data['GEO_POI_100M_CNT'] = len(rst_data[rst_data['distance_mtrs'] <= 100].index)
        
        for cat in categories: 
            cat_data = rst_data[rst_data.poi_category.eq(cat)]
            
            field_name_base = 'GEO_'+re.sub(' ','_',cat)
            
            #Count - 300 mts
            field_name = field_name_base + '_300M_CNT'
            value = len(cat_data.index)
            if value > 0:
                rst_forecast_data[field_name] = value 
            else:
                rst_forecast_data[field_name] = 0
            
            #Count - 100 mts
            field_name = field_name_base + '_100M_CNT'
            value = len(cat_data[cat_data['distance_mtrs'] <= 100].index)
            if value > 0:
                rst_forecast_data[field_name] = value 
            else:
                rst_forecast_data[field_name] = 0
            
            #Max Dist 
            field_name = field_name_base + '_MAX_DIST'
            value = cat_data.distance_mtrs.max() 
            if value > 0:
                rst_forecast_data[field_name] = value 
            else:
                rst_forecast_data[field_name] = 0
            
            #Min Dist
            field_name = field_name_base + '_MIN_DIST'
            value = cat_data.distance_mtrs.min() 
            if value > 0:
                rst_forecast_data[field_name] = value 
            else:
                rst_forecast_data[field_name] = 0
            
            
            #Avg Dist
            field_name = field_name_base + '_MEAN_DIST'
            value = cat_data.distance_mtrs.mean() 
            if value > 0:
                rst_forecast_data[field_name] = value 
            else:
                rst_forecast_data[field_name] = 0
            
        forecast_data.append(rst_forecast_data)
        
    forecast_data = pd.DataFrame(forecast_data)

    model = download_azure()
    rf_model = joblib.load(model)
    

    r = re.compile('GEO_.')

    x = forecast_data.loc[:,list(filter(r.match, forecast_data.columns))]


    class_predict = rf_model.predict(x)

    model.close()

    #print(class_predict)

    json_forecast = class_predict[0]
    json_pois = output.to_dict('records')
    cp = close_points('HONDURAS', 'POLLOLANDIA', lat, lon)
    
    res = {}
    res['forecast'] = json_forecast
    res['pois'] = json_pois
    res['close_points'] = cp

    #print(json.dumps(res))
    return json.dumps(res)
