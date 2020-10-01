import logging
import pandas as pd
import requests as rq
import json
import time
import re
import io
import sys
import joblib
from sklearn.ensemble import RandomForestClassifier
from geopy.distance import geodesic
from azure.storage.blob import BlockBlobService, PublicAccess
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

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
        response = NEX_MAIN(lat, lon, coordinates)
        return func.HttpResponse(response)
    else:
        return func.HttpResponse(
             "Pass the coordinates in the query string or in the request body for a personalized response.",
             status_code=200
        )

output = pd.DataFrame()

def find_places(lat, lon, category, pagetoken):
    api_url_base = 'https://maps.googleapis.com/maps/api/place/nearbysearch/json?'
    api_key = 'AIzaSyBkWP2Oj91ITxAPfHUHWRM4x2XkIxqqvf8'
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
        info['poi_type'] = category.upper()
        info['poi_ltt'] = result["geometry"]["location"]["lat"]
        info['poi_lgt'] = result["geometry"]["location"]["lng"]
        print(info)
        output = output.append(info, ignore_index=True)
    pagetoken = res.get("next_page_token", None)
    return pagetoken

def download_azure():
    DEST_FILE = 'cdp_model.sav'
    # Create the BlockBlockService that is used to call the Blob service for the storage account
    block_blob_service = BlockBlobService(account_name='gpistore', account_key='zfKM5R0PuPwR0F+pPsgs5BW/AQjAxv5fwKojoP2W38II++qfT6e+axFrRAcTOmKi/8U0tyJbrB2A3XCd7W7o6A==')

    # Create a container called 'quickstartblobs'.
    container_name ='cmia'
    blob_name='cdp_model.sav'

    stream = io.BytesIO()
    block_blob_service.get_blob_to_stream(container_name,blob_name,stream=stream,max_connections=2)
    #stream.close()

    return stream

def NEX_MAIN(lat, lon, coordinates):
    
    model_file = ''
    categories = ['atm', 'bank', 'bus_station', 'cafe', 'church', 'convenience_store', 'department_store', 'electronics_store', 'hospital', 'local_government_office', 'establishment', 'parking', 'police', 'restaurant', 'school', 'shopping_mall', 'store', 'university']
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
    print(output)

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

    regex = r'(?:BARBER|BEAUTY|BELLEZA|PELUQUER|NAILS)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'BARBERIA BELLEZA'

    regex = r'(?:IGLESIA|TEMPLO)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'IGLESIA'
               
    regex = r'(?:HOSPITAL|IGSS|CENTRO MEDICO|APROFAM|CENTRO DE SALUD|PUESTO DE SALUD|SANATORIO|HEALTH CENTER|EMERGENCIA|CIRUGIA|PEDIATRICO|SANATORIUM)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'HOSPITAL'
        
    regex = r'(?:CLINIC|MEDI|OPTIC|ODONTO|LABORATORIO|DR )'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'CLINICA DE SALUD'

    regex = r'(?:BUS |TRANSMETRO|TRANSURBANO|BUS STATION|AUTOBUSES|TERMINAL|ESTACION DE BUS|PARADA DE )'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'PARADA DE BUS'

    regex = r'(?:MANUALIDADES|LIBRERIA|PAPELERIA)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'LIBRERIA'

    regex = r'(?:PARQUEO|ESTACIONAMIENTO|PARKING)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'PARQUEO'

    regex = r'(?:ESTADIO|STADIUM)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'ESTADIO'

    regex = r'(?:USAC|UNIVERSIDAD|UMG|FACULTAD|UPANA)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'UNIVERSIDAD'

    regex = r'(?:HOTEL|PENSION|HOSPEDAJE)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'HOTEL'

    regex = r'(?:GASOLINERA|GASOLINA|ESTACION DE SERVICIO)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'GASOLINERA'

    regex = r'(?:MINI MUNI|MINISTRY PUBLIC|MUNICIPALITY|SECRETARIA DE|MUNICIPALIDAD|JUZGADO|TRIBUNAL|INACIF|RENAP|MAYCOM|ORGANISMO JUDICIAL|MINISTERIO PUBLICO|MUNICIPALIDAD)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'OFICINAS GUBERNAMENTALES'

    regex = r'(?:POLICE|PNC|COMISARIA|POLICIA)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'ESTACION POLICIAL'

    regex = '(?:ESTACION DE BOMBEROS)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'ESTACION DE BOMBEROS'

    regex = r'(?:SUPER 24|SUPER PUMA|CONVENIENCIA|GASO MARKET|TIENDAS? MASS)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'TIENDA DE CONVENIENCIA'

    regex = r'(?:CARNICERIA|POLLERIA|CASA DEL POLLO)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'VENTA DE CARNES'

    regex = r'(?:BANK)'
    output.loc[[re.search(regex, poi_type) is not None for poi_type in output['poi_type']], 'poi_category'] = 'BANCO'

    regex = r'(?:ATM)'
    output.loc[[re.search(regex, poi_type) is not None for poi_type in output['poi_type']], 'poi_category'] = 'CAJERO'

    regex = r'(?:PARQUE|CANCHA|ESTADIO|CAMPOS)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'PARQUE'

    regex = r'(?:COLEGIO|INSTITUTO|LICEO|SEMINARIO|CENTRO EDUCATIVO|ESCUELA)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'COLEGIO'

    regex = r'(?:COLEGIO|ACADEMIA|ESCUELA)(?:BAILE|FUTBOL|MUSICA)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'COLEGIO'

    regex = r'(?:ALMACENES TROPIGAS|AGENCIA% WAY|GALLO MAS GALLO|ELEKTRA|ELECKTRA|CURACAO|ALMACENES JAPON|LA CHAPINITA|TECNO FACIL|TIENDAS MAX|RADIOSHACK|INTELAF|ELECTRONICA PANAMERICANA|PANAMERICAN ELECTRONIC|ABM|EL DUENDE)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'ALMACEN DE ELECTRODOMESTICOS'

    regex = r'(?:POLLO CAMPERO|BURGER KING|MCDONALDS|PIZZA HUT|DOMINOS PIZZA|PAPA JOHNS|TACO BELL|HAMBURGUESAS BERLIN|HAMBURGUESAS DEL PUENTE|POLLO BUJO|LITTLE CAESARS|LITTLE CEASERS)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'CADENA QSR'

    regex = r'(?:LOS GAUCHITOS|PINULITO|POLLOLANDIA|POLLO LANDIA|QUEDELY|GUATEBURGER|GUATE BURGER|POLLO GRANJERO|POLLO EXPRESS)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'CADENA QSR POPULAR'

    regex = r'(?:ANTOJITOS|CAFETERIA|ASADOS|CHURRASCOS|HAMBURGUESAS|BURGER|PUPUSERIA|PUPUSAS|CARNITA|CEVICHE|SEVICHE|CHICHARRONE|TACO |SHUCO|HOT DOG|TAQUERIA|COMEDOR|ALMUERZOS)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'RESTAURANTE POPULAR'

    regex = r'(?:\bCAFE\b|EL CAFETALITO|BARISTA|STARBUCKS|COFFEE|COFFE|KAPE|PATSY|BAGEL|CAFFE|DUNKIN DONUTS|AMERICAN DOU)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'COFFEE SHOP'

    regex = r'(?:LA NEVERIA|HELADERIA|HELADO|POPS)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'HELADERIA'

    regex = r'(?:CANELLA|YAMAHA|MOTOS HONDA|MOTOS |MOTOCICLETAS|MASESA|TIENDA UMA|MOVESA|ITALIKA|HAOJUE|BAJAJ|SUZUKI)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'TIENDA MOTOCICLETAS'

    regex = r'(?:AGENCIA TIGO|\bTIGO\b|\bCLARO\b|AGENCIA CLARO|MOVISTAR)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'AGENCIA TELEFONIA'

    regex = r'(?:SUPERMERCADO|SUPER MERCADO|WALMART|DESPENSA FAMILIAR|LA TORRE|PAIZ|MAXI BODEGA|SUPER DEL BARRIO)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'SUPERMERCADO'

    regex = r'\b(?:MERCADO|CENMA)\b'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'MERCADO CANTONAL'

    regex = r'\b(?:FARMACIA|FARMACY|CRUZ VERDE|GALENO|MEYKOS)\b'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'FARMACIA'

    regex = r'(?:TIENDA|ABARROTERIA|ABARROTES|DEPOSITO|MAYOREO)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'TIENDA DE BARRIO'

    regex = r'(?:TORTILLERIA|TORTILLA)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'TORTILLERIA'

    regex = r'(?:PANADERIA|PANIFICADORA|PASTELERIA)'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'PANADERIA'

    regex = r'\b(?:CENTRO COMERCIAL|PLAZA|PASEO|CC|C\.C\.|C\. C\.|MALL|GRAN VIA|GRAN CENTRO|GALERIAS|MEGA CENTRO|METRO NORTE|PRADERA)\b'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'CENTRO COMERCIAL'

    regex = r'\b(?:MOTO|FIGUEPARTES|AQUARONI|KARS|HUESERA|RESPUESTOS|TALLER DE |ACEITERA|MOTOCENTRO|AUTOS|LUBRI|LLANTA|BATERIAS|COPHER)\b'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'REPUESTOS VEHICULOS'

    regex = r'\b(?:FERRE|CELASA|CONSTRUCC|FERRO)\b'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'FERRETERIA'

    regex = r'\b(?:AGROVET|AGROPE|AGRICUL)\b'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'AGROPECUARIA'

    regex = r'(?:local_government_office)'
    output.loc[[re.search(regex, poi_type) is not None for poi_type in output['poi_type']], 'poi_category'] = 'OFICINAS GUBERNAMENTALES'

    regex = r'(?:clothing_store|department_store)'
    output.loc[[re.search(regex, poi_type) is not None for poi_type in output['poi_type']], 'poi_category'] = 'ALMACEN'

    regex = r'(?:restaurant|meal_delivery)'
    output.loc[[re.search(regex, poi_type) is not None for poi_type in output['poi_type']], 'poi_category'] = 'OTROS RESTAURANTE'
    regex = r'\b(?:RESTAURANT)\b'
    output.loc[[re.search(regex, poi_name) is not None for poi_name in output['poi_name']], 'poi_category'] = 'OTROS RESTAURANTE'

    output.loc[[poi_category is None for poi_category in output['poi_category']], 'poi_category'] = 'OTROS SIN CLASIFICACION'


    output = output[['poi_name','poi_category', 'poi_lgt', 'poi_ltt', 'distance_mtrs']]
    output.drop_duplicates(subset=['poi_name', 'poi_ltt', 'poi_lgt'],inplace=True)


    categories = ['BARBERIA BELLEZA', 'IGLESIA', 'HOSPITAL', 'CLINICA DE SALUD', 'PARADA DE BUS', 'LIBRERIA', 'PARQUEO', 'ESTADIO', 'UNIVERSIDAD', 'HOTEL', 'GASOLINERA', 'OFICINAS GUBERNAMENTALES', 'ESTACION POLICIAL', 'ESTACION DE BOMBEROS', 'TIENDA DE CONVENIENCIA', 'VENTA DE CARNES', 'BANCO', 'CAJERO', 'PARQUE', 'COLEGIO', 'ALMACEN DE ELECTRODOMESTICOS', 'CADENA QSR', 'CADENA QSR POPULAR', 'RESTAURANTE POPULAR', 'COFFEE SHOP', 'HELADERIA', 'AGENCIA TELEFONIA', 'SUPERMERCADO', 'MERCADO CANTONAL', 'FARMACIA', 'TIENDA DE BARRIO', 'TORTILLERIA', 'PANADERIA', 'CENTRO COMERCIAL', 'REPUESTOS VEHICULOS', 'FERRETERIA', 'AGROPECUARIA', 'OFICINAS GUBERNAMENTALES', 'ALMACEN', 'OTROS RESTAURANTE', 'OTROS RESTAURANTE', 'OTROS SIN CLASIFICACION']

    forecast_data = {}
    forecast_data['GEO_POI_300M_CANT'] = len(output.index)
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

    model = download_azure()
    
    rf = joblib.load(model)

    pred = rf.predict(forecast_data)
    
    model.close()

    #'{"Forecast":"%s"}' % (pred[0])
    json_forecast = pred[0]
    json_pois = output.to_dict('records')

    res = {}
    res['forecast'] = json_forecast
    res['pois'] = json_pois

    #print(json.dumps(res))
    return json.dumps(res)