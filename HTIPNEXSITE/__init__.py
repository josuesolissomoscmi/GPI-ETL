import logging
import pandas as pd
import requests as rq
import json
import time
import re
import io
import sys
import joblib
import pyodbc 
from sklearn.ensemble import RandomForestClassifier
from geopy.distance import geodesic
from azure.storage.blob import BlockBlobService, PublicAccess
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    chain = req.params.get('chain')
    lat = req.params.get('lat')
    lon = req.params.get('lon')

    coordinates = (lat, lon)
    
    #if not name:
    #    try:
    #        req_body = req.get_json()
    #    except ValueError:
    #        pass
    #    else:
    #        name = req_body.get('name')

    if coordinates:
        response = NEX_MAIN(lat, lon, coordinates,chain)
        return func.HttpResponse(response)
    else:
        return func.HttpResponse(
             "Pass the coordinates in the query string or in the request body for a personalized response.",
             status_code=200
        )

output = pd.DataFrame()

def find_places(lat, lon, category, pagetoken):
    api_url_base = 'https://maps.googleapis.com/maps/api/place/nearbysearch/json?'
    api_key = 'AIzaSyC-JaHFmOnffZvVLJfxWo31NZCUVDqe27w'
    radius = 300
    global output

    if pagetoken is None:
        api_url_request = api_url_base + 'location=' + str(lat) + ',' + str(lon) + '&radius=' + str(radius) + '&type=' + category + '&key=' + api_key
    else:
        api_url_request = api_url_base + 'location=' + str(lat) + ',' + str(lon) + '&radius=' + str(radius) + '&type=' + category + '&key=' + api_key + pagetoken
    response = rq.get(api_url_request)
    res = json.loads(response.text)
    print(res)
    for result in res["results"]:
        # print(category)
        print(result)
        info = {}
        info['poi_id'] = result['place_id']
        info['poi_name'] = result["name"]
        info['poi_type'] = str(category).upper()
        info['poi_ltt'] = result["geometry"]["location"]["lat"]
        info['poi_lgt'] = result["geometry"]["location"]["lng"]
        print(info)
        output = output.append(info, ignore_index=True)
    pagetoken = res.get("next_page_token", None)
    return pagetoken

def download_azure(chain):
    DEST_FILE=''
    if chain == 'Casa_Del_Pollo':
        DEST_FILE = 'cdp_model.sav'
    else:
        DEST_FILE = 'pollolandia_model.sav'
    # Create the BlockBlockService that is used to call the Blob service for the storage account
    block_blob_service = BlockBlobService(account_name='gpistore', account_key='zfKM5R0PuPwR0F+pPsgs5BW/AQjAxv5fwKojoP2W38II++qfT6e+axFrRAcTOmKi/8U0tyJbrB2A3XCd7W7o6A==')

    # Create a container called 'quickstartblobs'.
    container_name ='cmia'
    #blob_name='cdp_model.sav'
    print(DEST_FILE)

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

def NEX_MAIN(lat, lon, coordinates,chain):
    
    model_file = ''
    categories = ['atm', 'bank', 'bus_station', 'cafe', 'church', 'convenience_store', 'department_store', 'electronics_store', 'hospital', 'local_government_office', 'parking', 'police', 'restaurant', 'school', 'shopping_mall', 'store', 'university']
    global output
    
    for category in categories:
        pagetoken = None
        while True:
            pagetoken = find_places(lat, lon, category, pagetoken=pagetoken)
            time.sleep(3)
            if not pagetoken:
                break

    output['distance_mtrs'] = None

    for index, row in output.iterrows():
        output.loc[index, 'distance_mtrs'] = geodesic(coordinates, (row['poi_ltt'], row['poi_lgt'])).meters

    # DATA CLEANING
    output['poi_lgt'] = round(output['poi_lgt'], 5)
    output['poi_ltt'] = round(output['poi_ltt'], 5)

    output['poi_name'] = [str(poi_name).upper() for poi_name in output['poi_name']]
    output['poi_type'] = [str(poi_type).upper() for poi_type in output['poi_type']]

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
    output = output[['poi_name','poi_category', 'poi_lgt', 'poi_ltt', 'distance_mtrs']]
    output.drop_duplicates(inplace=True)
    #print(output[['poi_name','poi_category', 'poi_lgt', 'poi_ltt']].sort_values(by=['poi_name']))

    categories = ['BARBERIA BELLEZA', 'IGLESIA', 'HOSPITAL', 'CLINICA DE SALUD', 'PARADA DE BUS', 'LIBRERIA', 'PARQUEO', 'ESTADIO', 'UNIVERSIDAD', 'HOTEL', 'GASOLINERA', 'OFICINAS GUBERNAMENTALES', 'ESTACION POLICIAL', 'ESTACION DE BOMBEROS', 'TIENDA DE CONVENIENCIA', 'VENTA DE CARNES', 'BANCO', 'CAJERO', 'PARQUE', 'COLEGIO', 'ALMACEN DE ELECTRODOMESTICOS', 'CADENA QSR', 'CADENA QSR POPULAR', 'RESTAURANTE POPULAR', 'COFFEE SHOP', 'HELADERIA', 'AGENCIA TELEFONIA', 'SUPERMERCADO', 'MERCADO CANTONAL', 'FARMACIA', 'TIENDA DE BARRIO', 'TORTILLERIA', 'PANADERIA', 'CENTRO COMERCIAL', 'REPUESTOS VEHICULOS', 'FERRETERIA', 'AGROPECUARIA', 'OFICINAS GUBERNAMENTALES', 'ALMACEN', 'OTROS RESTAURANTE', 'OTROS RESTAURANTE', 'OTROS SIN CLASIFICACION']

    forecast_data = {}
    forecast_data['GEO_POI_300M_CANT'] = len(output[output['distance_mtrs']<=300].index)
    forecast_data['GEO_POI_100M_CANT'] = len(output[output['distance_mtrs']<=100].index)

    for category in categories:
        print(category)
        
        data = output[output.poi_category.eq(category)]
        
        category = 'GEO_' + re.sub(' ','_', category)
        
        field_name = category + '_300M_CANT'
        value = len(data.index)
        print(value)
        if value > 0:
            forecast_data[field_name] = value
        else:
            forecast_data[field_name] = 0
        
        field_name = category + '_100M_CANT'
        value = len(data[data['distance_mtrs']<=100].index)
        if value > 0:
            forecast_data[field_name] = value
        else:
            forecast_data[field_name] = 0
        
        field_name = category + '_MIN_DIST'
        value = data.distance_mtrs.min()
        if value > 0:
            forecast_data[field_name] = value
        else:
            forecast_data[field_name] = 10000

    forecast_data = pd.DataFrame(data=forecast_data, index=[0])

    print(forecast_data)

    model = download_azure(chain)
    
    rf = joblib.load(model)

    pred = rf.predict(forecast_data)
    
    model.close()

    #'{"Forecast":"%s"}' % (pred[0])
    json_forecast = pred[0]
    json_pois = output.to_dict('records')

    cp = close_points('GUATEMALA', chain, lat, lon)
    
    res = {}
    res['forecast'] = json_forecast
    res['pois'] = json_pois
    res['close_points'] = cp


    #print(json.dumps(res))
    return json.dumps(res)


#print(NEX_MAIN(14.4970899, -90.5900806, (14.4970899, -90.5900806), 'Casa_Del_Pollo'))

#print(close_points('GUATEMALA', 'Casa_Del_Pollo', 14.4970899, -90.5900806))
