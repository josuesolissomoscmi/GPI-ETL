import logging
import azure.functions as func
from azure.storage.blob import BlockBlobService, PublicAccess
import requests
from io import BytesIO
from io import StringIO
import time
import pandas as pd
import pyodbc
import urllib.request
import urllib.parse
import datetime
from dateutil.relativedelta import relativedelta
import json


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
        respuesta = 'Opcion no definida'
        if name == 'COMMODITIES_PRICE_HISTORY_CF':
            respuesta = COMMODITIES_PRICE_HISTORY_CF()
        if name == 'COMMODITIES_PRICE_HISTORY_CC':
            respuesta = COMMODITIES_PRICE_HISTORY_CC()
        if name == 'COMMODITIES_PRICE':
            respuesta = COMMODITIES_PRICE()            
        if name == 'COMMODITIES_DOLLAR':
            respuesta = COMMODITIES_DOLLAR()
        if name == 'COMMODITIES_ETHANOL':
            respuesta = COMMODITIES_ETHANOL()
        if name == 'COMMODITIES_INDEX':
            respuesta = COMMODITIES_INDEX()
        if name == 'COMMODITIES_VI':
            respuesta = COMMODITIES_VI()
        if name == 'COMMODITIES_OI_VOLUME':
            respuesta = COMMODITIES_OI_VOLUME()
        if name == 'COMMODITIES_VI_5N':
            respuesta = COMMODITIES_VI_5N()            
        return func.HttpResponse(respuesta)
    else:
        return func.HttpResponse(
             "Please pass a name on the query string or in the request body",
             status_code=400
        )

base_url_prophetex = 'http://pxweb.dtn.com/PXWebSvc/PXServiceWeb.svc/'
history_method = 'GetDailyHistory'
headers = ['TickerSymbol', 'Date', 'Open', 'High', 'Low', 'Close', 'OI', 'Volume']
headers_iv = ['Date', 'SymbolATM', 'PriceATM', 'Symbol50D_C', 'Price50D_C', 'Symbol40D_C', 'Price40D_C', 'Symbol30D_C', 'Price30D_C', 'Symbol20D_C', 'Price20D_C', 'Symbol10D_C', 'Price10D_C', 'SymbolATM_C', 'PriceATM_C', 'SymbolATM_P', 'PriceATM_P', 'Symbol_10D_P', 'Price_10D_P', 'Symbol_20D_P', 'Price_20D_P', 'Symbol_30D_P', 'Price_30D_P', 'Symbol_40D_P', 'Price_40D_P', 'Symbol_50D_P', 'Price_50D_P','Skew']
headers_iv_5n = ['Date', 'ExpirationSymbol', 'SymbolATM', 'Close', 'PriceATM', 'Symbol50D_C', 'Price50D_C', 'Symbol40D_C', 'Price40D_C', 'Symbol30D_C', 'Price30D_C', 'Symbol20D_C', 'Price20D_C', 'Symbol10D_C', 'Price10D_C', 'SymbolATM_C', 'PriceATM_C', 'SymbolATM_P', 'PriceATM_P', 'Symbol_10D_P', 'Price_10D_P', 'Symbol_20D_P', 'Price_20D_P', 'Symbol_30D_P', 'Price_30D_P', 'Symbol_40D_P', 'Price_40D_P', 'Symbol_50D_P', 'Price_50D_P','Skew']
headers_oi = ['Symbol', 'Date', 'OI','Volume']
months_code = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']
date_format = '%m-%d-%Y'
sql_prices = "SELECT commodity, MAX(fecha) FROM(SELECT [Date] fecha, CASE WHEN (LEN(TickerSymbol)=5 OR LEN(TickerSymbol)=7) THEN RIGHT(LEFT(TickerSymbol, 2), 1) ELSE CASE WHEN (LEFT(TickerSymbol,1)='@') THEN RIGHT(LEFT(TickerSymbol, 3),2) ELSE LEFT(TickerSymbol,3) END END commodity FROM [ST_PROPHETX].[COMMODITIES_PRICE]) t GROUP BY commodity"
sql_future = "SELECT commodity, MAX(fecha) FROM(SELECT [Date] fecha, CASE WHEN (LEN(TickerSymbol)=5 OR LEN(TickerSymbol)=7) THEN RIGHT(LEFT(TickerSymbol, 2), 1) ELSE CASE WHEN (LEFT(TickerSymbol,1)='@') THEN RIGHT(LEFT(TickerSymbol, 3),2) ELSE LEFT(TickerSymbol,3) END END commodity FROM [ST_PROPHETX].[COMMODITIES_PRICE_HISTORY_CF]) t GROUP BY commodity"
sql_continuo = "SELECT 	commodity, MAX(fecha) FROM(SELECT [Date] fecha,	REPLACE(LEFT(TickerSymbol, LEN(TickerSymbol)-2), '@', '') commodity FROM [ST_PROPHETX].[COMMODITIES_PRICE_HISTORY_CC] ) t GROUP BY commodity"
sql_dollar_index = "SELECT REPLACE(LEFT(TickerSymbol,3), '@', '') commodity, MAX([Date]) FROM [ST_PROPHETX].[COMMODITIES_DOLLAR] GROUP BY TickerSymbol"
sql_ethanol_index = "SELECT REPLACE(LEFT(TickerSymbol,3), '@', '') commodity, MAX([Date]) FROM [ST_PROPHETX].[COMMODITIES_ETHANOL] GROUP BY TickerSymbol"
sql_commodity_index = "SELECT REPLACE(LEFT(TickerSymbol,3), '@', '') commodity, MAX([Date]) FROM [ST_PROPHETX].[COMMODITIES_INDEX] GROUP BY TickerSymbol"
sql_iv = "SELECT commodity, MAX([fecha]) FROM(SELECT [Date] fecha, CASE WHEN (LEN(SymbolATM)=5 OR LEN(SymbolATM)=7) THEN RIGHT(LEFT(SymbolATM, 2), 1) ELSE CASE WHEN (LEFT(SymbolATM,1)='@') THEN RIGHT(LEFT(SymbolATM, 3),2) ELSE LEFT(SymbolATM,3) END END commodity FROM [ST_PROPHETX].[COMMODITIES_VI]) t GROUP BY commodity"
sql_iv_next = "SELECT commodity, MAX([fecha]) FROM(SELECT [Date] fecha, CASE WHEN (LEN(SymbolATM)=5 OR LEN(SymbolATM)=7) THEN RIGHT(LEFT(SymbolATM, 2), 1) ELSE CASE WHEN (LEFT(SymbolATM,1)='@') THEN RIGHT(LEFT(SymbolATM, 3),2) ELSE LEFT(SymbolATM,3) END END commodity FROM [ST_PROPHETX].[COMMODITIES_VI_N5]) t GROUP BY commodity"
sql_oi_vol = "SELECT commodity, MAX(fecha) FROM (SELECT [Date] fecha, CASE WHEN (LEN(Symbol)=7) THEN REPLACE(LEFT(Symbol,2), '@', '') ELSE REPLACE(LEFT(Symbol,3), '@', '') END commodity FROM [ST_PROPHETX].[COMMODITIES_OI_VOLUME]) t GROUP BY commodity"
saltos_call_put = 5

expiration_months_market_commodities = {
        'C': [['H', 'K', 'N', 'U', 'Z'], 'CBOT'],
        'SM': [['F', 'H', 'K', 'N', 'Q', 'U', 'V', 'Z'], 'CBOT'],
        'S': [['F', 'H', 'K', 'N', 'Q', 'U', 'X'], 'CBOT'],
        'W': [['H', 'K', 'N', 'U', 'Z'], 'CBOT'],
        'KW': [['H', 'K', 'N', 'U', 'Z'], 'KCBT'],
        # Para la historia poner market MGEX
        #'MW': [['H', 'K', 'N', 'U', 'Z'], 'MGEX'],
        'MW': [['H', 'K', 'N', 'U', 'Z'], ''],
        'BO': [['F', 'H', 'K', 'N', 'Q', 'U', 'V', 'Z'], 'CBOT'],
        'QCL': [['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z'], ''],
        'DX': [['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z'], ''],
        'AC': [['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z'], ''],
        'AE': [['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z'], '']
    }

def get_next2_expiration_dates(current_month, current_year, expiration_months, months_code):
    expiration_symbols = []
    #Se detiene al obtener 2 expiraciones
    while(len(expiration_symbols) < 2):
        #Se obtiene el codigo del mes si es 8 nos devolvera Q
        current_month_code = months_code[current_month-1]
        #Si el mes esta dentro de los meses de expiracion de ese commoditie lo agrega si no sigue buscando
        if (current_month_code in expiration_months):
            expiration_symbols.append(current_month_code+str(current_year))
        current_month += 1
        #Cuando el mes actual sea 13, lo reinicia a 1 y pasa al siguiente año
        if (current_month >= 13):
            current_month = 1
            current_year += 1
    return expiration_symbols

def get_expiration_symbol(current_date, symbol, expiration_months, months_code, full_digits_year):
    expiration_symbol = ""
    #Le sumamos 6 meses a la fecha actual current_date=01/02/2020 a expiration_date=01/08/2020
    expiration_date = current_date + relativedelta(months=+6)    
    n_day = expiration_date.day
    n_month = expiration_date.month
    if(full_digits_year):
        # AÑO EN 4 DIGITOS PARA VI
        n_year_wc = expiration_date.year
    else:
        # AÑO EN 2 DIGITOS PARA CONTRATOS FUTUROS Y CONTRATOS CONTINUOS
        n_year_wc = int(expiration_date.strftime('%y'))
    
    # Obtiene las siguientes 2 expiraciones de 6 meses adelante ejemplo U20 y Z20 partiendo del ejemplo fecha actual 01/02/2020
    next2_expiration_dates = get_next2_expiration_dates(n_month, n_year_wc, expiration_months, months_code)
    # Devolvera el simbolo del contrato actual a 6 meses en este caso es Q20 por octubre 2020
    current_expiration_date = months_code[n_month-1]+str(n_year_wc)
    #si el mes es 12 se cambia a 1 y se pasa al siguiente año
    #de lo contrario le asigna el mes actual a 6 meses en este caso 8
    if (n_month == 12):
        next_expiration_date = months_code[0]+str(n_year_wc+1)
    else:
        next_expiration_date = months_code[n_month]+str(n_year_wc)
    #(Q20 == U20)  or (U20 == U20 and 1 > 20)
    # el dia 20 es porque es la fecha en que un contrato vence 
    # en este caso devolvera U20 porque se quedara en el primer contrato, si la fecha es mayor a 20 se saltara al siguiente contrato
    if ((current_expiration_date == next2_expiration_dates[0]) or (next_expiration_date == next2_expiration_dates[0] and n_day > 20)):
        #@WZ20
        expiration_symbol = symbol+next2_expiration_dates[1]
    else:
        #@WU20
        expiration_symbol = symbol+next2_expiration_dates[0]
    return expiration_symbol
    
def get_expiration_symbols(start_date, end_date, symbol, expiration_months, months_code, full_digits_year):
    symbols = []
    while (start_date < end_date):
        row = []
        #Se recibe la fecha de inicio ejemplo: 01/02/2020
        start_date_str = start_date.strftime(date_format)
        #Le sumamos 6 meses a la fecha de inicio resultado: 01/08/2020
        expiration_date = start_date + relativedelta(months=+6)
        expiration_date_str = expiration_date.strftime(date_format)
        row.append(start_date_str)
        row.append(expiration_date_str)
        #
        expiration_symbol = get_expiration_symbol(start_date, symbol, expiration_months, months_code, full_digits_year)
        row.append(expiration_symbol)
        start_date = start_date + datetime.timedelta(days=1)
        symbols.append(row)
    return symbols

def get_expiration_symbols_ranges(symbols):
    query_ranges = []
    actual_value = ["","",""]    
    new_range=[]
    if (len(symbols) > 0):
        actual_value = symbols[0]
        new_range.append(actual_value[0])
    if (len(symbols) == 0):
        return query_ranges
    for i in range(1,len(symbols)):
        if(symbols[i][2] != actual_value[2]):
            new_range.append(symbols[i-1][0])
            new_range.append(actual_value[2])
            query_ranges.append(new_range)
            actual_value = symbols[i]
            new_range=[]
            new_range.append(actual_value[0])
    new_range.append(symbols[len(symbols)-1][0])
    new_range.append(actual_value[2])
    query_ranges.append(new_range)
    return query_ranges

def get_futures_prices(symbols, market):
    futures_data = pd.DataFrame(columns=headers)
    for i in range(len(symbols)):
        time.sleep(2)
        url = base_url_prophetex + history_method
        params = urllib.parse.urlencode(
            {
                'UserID':'ws@mfgrains.com',
                'Password': 'Kr5o8N',
                'Symbol': symbols[i][2],
                'StartDate': symbols[i][0],
                'EndDate': symbols[i][1],
                'Limit': 8000,
                'Market': market,
                'Format': 'CSV'
            }
        )
        req = urllib.request.urlopen(url + '?%s' % params)
        print(req.geturl())
        content = req.read()
        
        
        df = pd.read_csv(StringIO(content.decode()), header=0)
        print(headers)
        #Si no hay informacion se genera el dataframe solo con encabezados
        if not df.empty:
           futures_data = futures_data.append(df[headers])
    return futures_data

def get_last_record_date(sql):
    server = 'grainpredictive.database.windows.net'
    database = 'gpi'
    username = 'gpi'
    password = 'Cmi@2019$A'
    driver= '{ODBC Driver 17 for SQL Server}'
    cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    cursor.execute(sql)
    dates = []
    for row in cursor:
        date_row = []
        commodity = row[0]
        date = row[1]
        date_row.append(commodity)
        date_row.append(date)
        dates.append(date_row)
    return dates

def get_close_values(commodity, last_date):
    start_date = last_date + datetime.timedelta(days=1)
    end_date = datetime.datetime.now() - datetime.timedelta(days=1)
    #Si la fecha inicial es mayor a la fecha final no debe devolver datos
    if (start_date.strftime("%Y-%m-%d") > end_date.strftime("%Y-%m-%d")):
        futures_data = pd.DataFrame(columns=headers)
        return futures_data
    print("Commodity: "+commodity)
    print("From "+start_date.strftime("%Y-%m-%d")+" to "+end_date.strftime("%Y-%m-%d"))
    commodity_query = commodity
    if (commodity_query != 'QCL'):
        commodity_query = '@'+commodity_query
    expiration_months_market_commodity = expiration_months_market_commodities.get(commodity)
    #Obtener los simbolos de expiracion
    symbols = get_expiration_symbols(start_date, end_date, commodity_query, expiration_months_market_commodity[0], months_code, full_digits_year=False)
    symbols_ranges = get_expiration_symbols_ranges(symbols)
    if (len(symbols_ranges)==0):
        futures_data = pd.DataFrame(columns=headers)
        return futures_data
    close_values = get_futures_prices(symbols_ranges, expiration_months_market_commodity[1])
    return close_values

def get_close_values_continuo(commodity, last_date):
    start_date = last_date + datetime.timedelta(days=1)
    end_date = datetime.datetime.now() - datetime.timedelta(days=1)
    #Si la fecha inicial es mayor a la fecha final no debe devolver datos
    if (start_date.strftime("%Y-%m-%d") > end_date.strftime("%Y-%m-%d")):
        futures_data = pd.DataFrame(columns=headers)
        return futures_data    
    print("Commodity: "+commodity)
    print("From "+start_date.strftime("%Y-%m-%d")+" to "+end_date.strftime("%Y-%m-%d"))
    commodity_query = commodity
    if (commodity_query != 'QCL'):
        commodity_query = '@'+commodity_query
    #"expiration_months_market_commodities" Es un listado de donde se obtiene los meses de expiracion de contratos en funcion del commoditie que se pasa por parametro
    expiration_months_market_commodity = expiration_months_market_commodities.get(commodity)
    #Se forma el rango de fecha y comoditie que se quiere obtener, ejemplo: symbols_ranges = ['02-01-2020', '02-02-2020', '@W@C']
    symbols_ranges = []
    symbol_range = []
    symbol_range.append(start_date.strftime(date_format))
    symbol_range.append(end_date.strftime(date_format))
    symbol_range.append(commodity_query+"@C")
    symbols_ranges.append(symbol_range)
    #Se hace la consulta a la pagina, se pasa el rango de fecha y el commoditie, asi como el mercado como parametro
    close_values = get_futures_prices(symbols_ranges, expiration_months_market_commodity[1])
    return close_values

def get_close_values_iv(commodity, last_date):
    start_date = last_date + datetime.timedelta(days=1)
    end_date = datetime.datetime.now()
    print("Commodity: "+commodity)
    print("From "+start_date.strftime("%Y-%m-%d")+" to "+end_date.strftime("%Y-%m-%d"))
    commodity_query = commodity
    if (commodity_query != 'QCL'):
        commodity_query = '@'+commodity_query
    expiration_months_market_commodity = expiration_months_market_commodities.get(commodity)
    symbols = get_expiration_symbols(start_date, end_date, commodity_query, expiration_months_market_commodity[0], months_code, full_digits_year=True)
    symbols_ranges = get_expiration_symbols_ranges(symbols)
    close_values = get_futures_prices_iv(symbols_ranges, expiration_months_market_commodity[1])
    return close_values

def get_futures_prices_iv(symbols, market):
    atm_data = pd.DataFrame(columns=['Date', 'TickerSymbol', 'Close'])
    for i in range(len(symbols)):
        time.sleep(2)
        url = base_url_prophetex + history_method
        params = urllib.parse.urlencode(
            {
                'UserID':'ws@mfgrains.com',
                'Password': 'Kr5o8N',
                'Symbol': symbols[i][2],
                'StartDate': symbols[i][0],
                'EndDate': symbols[i][1],
                'Limit': 8000,
                'Market': market,
                'Format': 'CSV'
            }
        )
        req = urllib.request.urlopen(url + '?%s' % params)
        content = req.read()
        df = pd.read_csv(StringIO(content.decode()), header=0)

        if df.empty:
            continue

        df['ATM'] = df['Close'].apply(lambda x: round(x/100,1)*1000)
        atm_data = atm_data.append(df[['Date', 'TickerSymbol', 'Close', 'ATM']])
    calls_puts = []
    for index, row in atm_data.iterrows():
        day_calls_puts = []
        day_calls_puts.append(row['Date'])
        day_calls_puts.append(row['TickerSymbol'])
        day_calls_puts.append(row['ATM'])        
        for i in range(saltos_call_put,0,-1):
            salto_call = row['TickerSymbol'] + 'C' + str(int(row['ATM']+(100*i))) + '.IV'
            day_calls_puts.append(salto_call)
        atm_call = row['TickerSymbol'] + 'C' + str(int(row['ATM'])) + '.IV'        
        day_calls_puts.append(atm_call)
        atm_put = row['TickerSymbol'] + 'P' + str(int(row['ATM'])) + '.IV'
        day_calls_puts.append(atm_put)
        for i in range(saltos_call_put):
            salto_put = row['TickerSymbol'] + 'P' + str(int(row['ATM']-(100*(i+1)))) + '.IV'
            day_calls_puts.append(salto_put)
        calls_puts.append(day_calls_puts)
    iv_data = pd.DataFrame(columns=headers_iv)
    for i in range(len(calls_puts)):
        time.sleep(2)
        url = base_url_prophetex + history_method
        params = urllib.parse.urlencode(
            {
                'UserID':'ws@mfgrains.com',
                'Password': 'Kr5o8N',
                'Symbol': ','.join(calls_puts[i][3:]),
                'StartDate': calls_puts[i][0],
                'EndDate': calls_puts[i][0],
                'Limit': 8000,
                'Market': market,
                'Format': 'CSV'
            }
        )
        req = urllib.request.urlopen(url + '?%s' % params)
        content = req.read()
        df = pd.read_csv(StringIO(content.decode()), header=0)
        df = df.pivot(index='Date', columns='TickerSymbol', values='Close')
        for index, row in df.iterrows():
            iv_row = pd.DataFrame(
                {
                    'Date': calls_puts[i][0],
                    'SymbolATM': calls_puts[i][1],
                    'PriceATM': calls_puts[i][2],
                    'Symbol50D_C': calls_puts[i][3],
                    'Price50D_C': row[5],
                    'Symbol40D_C': calls_puts[i][4],
                    'Price40D_C': row[4],
                    'Symbol30D_C': calls_puts[i][5],
                    'Price30D_C': row[3],
                    'Symbol20D_C': calls_puts[i][6],
                    'Price20D_C': row[2],
                    'Symbol10D_C': calls_puts[i][7],
                    'Price10D_C': row[1],
                    'SymbolATM_C': calls_puts[i][8],
                    'PriceATM_C': row[0],
                    'SymbolATM_P': calls_puts[i][9],
                    'PriceATM_P': row[11],
                    'Symbol_10D_P': calls_puts[i][10],
                    'Price_10D_P': row[10],
                    'Symbol_20D_P': calls_puts[i][11],
                    'Price_20D_P': row[9],
                    'Symbol_30D_P': calls_puts[i][12],
                    'Price_30D_P': row[8],
                    'Symbol_40D_P': calls_puts[i][13],
                    'Price_40D_P': row[7],
                    'Symbol_50D_P': calls_puts[i][14],
                    'Price_50D_P': row[6]
                }, index=[0]
            )
            iv_data = iv_data.append(iv_row, ignore_index=True)
    iv_data['call']=iv_data[['Price50D_C','Price40D_C','Price30D_C','Price20D_C','Price10D_C','PriceATM_C']].sum(axis=1, skipna=True)
    iv_data['put']=iv_data[['PriceATM_P','Price_10D_P','Price_20D_P','Price_30D_P','Price_40D_P','Price_50D_P']].sum(axis=1, skipna=True)
    iv_data['Skew']=iv_data['call']-iv_data['put']
    return iv_data

def get_next5_expiration_symbols(date_current, expiration_months, months_code, symbol):
    months_symbols = []
    n_month = date_current.month
    n_year = date_current.year    
    while(len(months_symbols)<5):
        n_month += 1
        if (n_month == 13):
            n_year += 1
            n_month = 1        
        current_month = months_code[n_month-1]
        if (current_month in expiration_months):
            months_symbols.append(symbol+current_month+str(n_year))
    return months_symbols

def get_next5_expiration_symbols_date(start_date, end_date, symbol, expiration_months):
    symbols = []
    while (start_date < end_date):
        row = []
        start_date_str = start_date.strftime(date_format)
        row.append(start_date_str)
        last_month_date = start_date + relativedelta(day=1, months=+1, days=-1)
        last_month_date_str = last_month_date.strftime(date_format)
        row.append(last_month_date_str)        
        symbols_month = get_next5_expiration_symbols(start_date, expiration_months, months_code, symbol)
        symbols_month_str = ','.join(map(str, symbols_month))
        row.append(symbols_month_str)
        start_date = start_date + relativedelta(months=+1)
        symbols.append(row)
    return symbols

def get_next5_expiration_symbols_date_oi(start_date, end_date, symbol, expiration_months):
    symbols = []
    while (start_date < end_date):
        row = []
        start_date_str = start_date.strftime(date_format)
        row.append(start_date_str)
        #last_month_date = start_date + relativedelta(day=1, months=+1, days=-1)
        #last_month_date_str = last_month_date.strftime(date_format)
        end_date_str = end_date.strftime(date_format)
        row.append(end_date_str)
        #row.append(last_month_date_str)        
        symbols_month = get_next5_expiration_symbols(start_date, expiration_months, months_code, symbol)
        symbols_month_str = ','.join(map(str, symbols_month))
        row.append(symbols_month_str)
        start_date = start_date + relativedelta(months=+1)
        symbols.append(row)
    return symbols

def get_close_values_oi_vol(symbols, market,symbol):
    oi_data = pd.DataFrame(columns=['Symbol','Date', 'OI','Volume'])
    for i in range(len(symbols)):
        time.sleep(2)
        url = base_url_prophetex + history_method
        params = urllib.parse.urlencode(
            {
                'UserID':'ws@mfgrains.com',
                'Password': 'Kr5o8N',
                'Symbol': symbols[i][2],
                'StartDate': symbols[i][0],
                'EndDate': symbols[i][1],
                'Limit': 8000,
                'Market': market,
                'Format': 'CSV'
            }
        )
        req = urllib.request.urlopen(url + '?%s' % params)
        print(req.geturl())
        content = req.read()
        df = pd.read_csv(StringIO(content.decode()), header=0)        
        oi_row = pd.DataFrame({'OI': df.groupby(['Date'])['OI'].sum(), 'Volume': df.groupby(['Date'])['Volume'].sum()}).reset_index()
        oi_data = oi_data.append(oi_row, ignore_index=True) 
        oi_data['Symbol'] = symbol.replace('@','')
        oi_data = oi_data[['Symbol','Date', 'OI','Volume']]       
    return oi_data

def get_oi_volume(commodity, last_date):
    start_date = last_date + datetime.timedelta(days=1)
    end_date = datetime.datetime.now() - datetime.timedelta(days=1)
    #si ambas fechas son iguales evitamos que traiga la informacion del dia actual, porque no tendra el open interest
    if(start_date.strftime("%Y-%m-%d") > end_date.strftime("%Y-%m-%d")):
          oi_data = pd.DataFrame(columns=['Symbol','Date', 'OI','Volume'])  
          return oi_data
    print("Commodity: "+commodity)
    print("From "+start_date.strftime("%Y-%m-%d")+" to "+end_date.strftime("%Y-%m-%d"))
    commodity_query = commodity
    if (commodity_query != 'QCL'):
        commodity_query = '@'+commodity_query
    expiration_months_market_commodity = expiration_months_market_commodities.get(commodity)
    symbols = get_next5_expiration_symbols_date_oi(start_date, end_date, commodity_query, expiration_months_market_commodity[0])
    close_values = get_close_values_oi_vol(symbols, expiration_months_market_commodity[1],commodity)
    return close_values

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
    return f'Extraccion de ProphetX  de {file_name} exitosa.'

def COMMODITIES_PRICE_HISTORY_CF():
    records = get_last_record_date(sql_future)
    data = pd.DataFrame(columns=headers)
    for i in range(len(records)):
        commodity = records[i][0]
        last_date = records[i][1]
        values = get_close_values(commodity, last_date)
        data = data.append(values[headers])
    data['actualizacion'] = datetime.datetime.now()
    if data.empty:
        r = 'False'
    else:
        r = 'True'
        data = data.replace('---','0')  
    upload_azure(data, 'COMMODITIES_PRICE_HISTORY_CF')
    return r    
    
def COMMODITIES_PRICE_HISTORY_CC():
    records_continuo = get_last_record_date(sql_continuo)
    data = pd.DataFrame(columns=headers)
    for i in range(len(records_continuo)):
        commodity = records_continuo[i][0]
        last_date = records_continuo[i][1]
        values_continuo = get_close_values_continuo(commodity, last_date)
        data = data.append(values_continuo[headers])
    data['actualizacion'] = datetime.datetime.now()
    
    if data.empty:
        r = 'False'
    else:
        r = 'True'
        data = data.replace('---','0')    
    
    upload_azure(data, 'COMMODITIES_PRICE_HISTORY_CC')
    return r

def get_futures_prices_next_expirations(n_expirations, symbol, date):
    start_date = date + datetime.timedelta(days=1)
    end_date = datetime.datetime.now() - datetime.timedelta(days=1)
    #Si la fecha inicial es mayor a la fecha final no debe devolver datos
    if (start_date.strftime("%Y-%m-%d") > end_date.strftime("%Y-%m-%d")):
        futures_data = pd.DataFrame(columns=headers)
        return futures_data

    futures_data = pd.DataFrame(columns=['TickerSymbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'OI', 'ExpirationSymbol'])
    for i in range(n_expirations):
        time.sleep(2)
        url = base_url_prophetex + history_method
        params = urllib.parse.urlencode(
            {
                'UserID':'ws@mfgrains.com',
                'Password': 'Kr5o8N',
                'Symbol': '@'+symbol+'@'+str(i+1),
                'StartDate': start_date,
                'Limit': 8000,
                'Format': 'CSV'
            }
        )
        req = urllib.request.urlopen(url + '?%s' % params)
        print(req.geturl())
        content = req.read()        
        df = pd.read_csv(StringIO(content.decode()), header=0)
        df['ExpirationSymbol'] = '@'+symbol+'@'+str(i+1)
        futures_data = futures_data.append(df)
    return futures_data

def COMMODITIES_PRICE():
    records = get_last_record_date(sql_prices)
    data = pd.DataFrame(columns=headers)
    for i in range(len(records)):
        commodity = records[i][0]
        last_date = records[i][1]

        #@S Frijol de Soya (7 expiraciones)
        soybeans_7exp = ["S"]
        #@SM Harina de Soya y @BO Aceite de Soya (8 expiraciones)
        soybeans_8exp = ["SM", "BO"]
  
        if commodity in soybeans_7exp:
            data_next_n_expirations = get_futures_prices_next_expirations(7,commodity,last_date)  
        elif commodity in soybeans_8exp:
            data_next_n_expirations = get_futures_prices_next_expirations(8,commodity,last_date) 
        else:
            data_next_n_expirations = get_futures_prices_next_expirations(6,commodity,last_date)                
        data = data.append(data_next_n_expirations[headers])
    data['actualizacion'] = datetime.datetime.now()
    
    if data.empty:
        r = 'False'
    else:
        r = 'True'
        data = data.replace('---','0')    
    
    upload_azure(data, 'COMMODITIES_PRICE')
    return r
    
def COMMODITIES_DOLLAR():  
    records_dollar_index = get_last_record_date(sql_dollar_index)
    data = pd.DataFrame(columns=headers)
    commodity = records_dollar_index[0][0]
    last_date = records_dollar_index[0][1]
    values_dollar_index = get_close_values_continuo(commodity, last_date)
    data = data.append(values_dollar_index[headers])
    data['actualizacion'] = datetime.datetime.now()
    
    if data.empty:
        r = 'False'
    else:
        r = 'True'
        data = data.replace('---','0')    
    
    upload_azure(data, 'COMMODITIES_DOLLAR')
    return r    

#fjsolis - ethanol
def COMMODITIES_ETHANOL():  
    records_ethanol_index = get_last_record_date(sql_ethanol_index)
    data = pd.DataFrame(columns=headers)
    commodity = records_ethanol_index[0][0]
    last_date = records_ethanol_index[0][1]
    values_ethanol_index = get_close_values_continuo(commodity, last_date)
    data = data.append(values_ethanol_index[headers])
    data['actualizacion'] = datetime.datetime.now()
    
    if data.empty:
        r = 'False'
    else:
        r = 'True'
        data = data.replace('---','0')    
    
    upload_azure(data, 'COMMODITIES_ETHANOL')
    return r    

def COMMODITIES_INDEX():    
    records_commodity_index = get_last_record_date(sql_commodity_index)
    data = pd.DataFrame(columns=headers)
    commodity = records_commodity_index[0][0]
    last_date = records_commodity_index[0][1]
    values_commodity_index = get_close_values_continuo(commodity, last_date)
    data = data.append(values_commodity_index[headers])
    data['actualizacion'] = datetime.datetime.now()    
   
    if data.empty:
        r = 'False'
    else:
        r = 'True'
        data = data.replace('---','0')       
    
    upload_azure(data, 'COMMODITIES_INDEX')
    return r    

def COMMODITIES_VI():    
    records_iv = get_last_record_date(sql_iv)
    data = pd.DataFrame(columns=headers_iv)
    for i in range(len(records_iv)):
        commodity = records_iv[i][0]
        last_date = records_iv[i][1]
        values_iv = get_close_values_iv(commodity, last_date)
        data = data.append(values_iv[headers_iv])
    data['actualizacion'] = datetime.datetime.now()
    
    if data.empty:
        r = 'False'
    else:
        r = 'True'
        data = data.replace('---','0')       
    
    upload_azure(data, 'COMMODITIES_VI')
    return r    
 
def COMMODITIES_OI_VOLUME():
    records_oi_vol = get_last_record_date(sql_oi_vol)
    data = pd.DataFrame(columns=headers_oi)
    for i in range(len(records_oi_vol)):
        commodity = records_oi_vol[i][0]
        last_date = records_oi_vol[i][1]
        values_oi_vol = get_oi_volume(commodity, last_date)
        data = data.append(values_oi_vol[headers_oi])
    data['actualizacion'] = datetime.datetime.now()
    
    if data.empty:
        r = 'False'
    else:
        r = 'True'
        data = data.replace('---','0')   

    upload_azure(data, 'COMMODITIES_OI_VOLUME')
    return r

def get_volatilidad_implicita_next_expirations(n_expirations, symbol, date):
    start_date = date + datetime.timedelta(days=1)
    end_date = datetime.datetime.now() - datetime.timedelta(days=1)
    #Si la fecha inicial es mayor a la fecha final no debe devolver datos
    if (start_date.strftime("%Y-%m-%d") > end_date.strftime("%Y-%m-%d")):
        futures_data = pd.DataFrame(columns=headers_iv_5n)
        return futures_data
    
    iv = pd.DataFrame(columns=headers_iv_5n)
    tp = []
    for i in range(n_expirations):
        atm_data = pd.DataFrame(columns=['Date', 'TickerSymbol', 'Close'])
        time.sleep(2)
        url = base_url_prophetex + history_method
        params = urllib.parse.urlencode(
            {
                'UserID':'ws@mfgrains.com',
                'Password': 'Kr5o8N',
                'Symbol': '@'+symbol+'@'+str(i+1),
                'StartDate': start_date,                
                'Format': 'CSV'
            }
        )
        req = urllib.request.urlopen(url + '?%s' % params)
        print(req.geturl())
        logging.info(req.geturl())
        content = req.read()
        df = pd.read_csv(StringIO(content.decode()), header=0)
        if symbol == 'SM':
            df['ATM'] = df['Close'].apply(lambda x: round(x/100,1)*10000)
        elif symbol == 'BO':
            df['ATM'] = df['Close'].apply(lambda x: round(x/10,1)*10000)
        else:    
            df['ATM'] = df['Close'].apply(lambda x: round(x/100,1)*1000)
        atm_data = atm_data.append(df[['Date', 'TickerSymbol', 'Close', 'ATM']])
        calls_puts = []
        for index, row in atm_data.iterrows():
            day_calls_puts = []
            day_calls_puts.append(row['Date'])
            day_calls_puts.append(row['TickerSymbol'])
            day_calls_puts.append(row['Close'])
            day_calls_puts.append(row['ATM'])
            for j in range(saltos_call_put,0,-1):
                if symbol == 'S':
                    salto_call = row['TickerSymbol'] + 'C' + str(int(row['ATM']+(200*j))) + '.IV'
                elif symbol == 'SM':
                    salto_call = row['TickerSymbol'] + 'C' + str(int(row['ATM']+(1000*j))) + '.IV'
                elif symbol == 'BO':
                    salto_call = row['TickerSymbol'] + 'C' + str(int(row['ATM']+(500*j))) + '.IV'
                else:
                    salto_call = row['TickerSymbol'] + 'C' + str(int(row['ATM']+(100*j))) + '.IV'
                day_calls_puts.append(salto_call)
            atm_call = row['TickerSymbol'] + 'C' + str(int(row['ATM'])) + '.IV'        
            day_calls_puts.append(atm_call)
            atm_put = row['TickerSymbol'] + 'P' + str(int(row['ATM'])) + '.IV'
            day_calls_puts.append(atm_put)
            for j in range(saltos_call_put):
                if symbol == 'S':
                    salto_put = row['TickerSymbol'] + 'P' + str(int(row['ATM']-(200*(j+1)))) + '.IV'
                elif symbol == 'SM':
                    salto_put = row['TickerSymbol'] + 'P' + str(int(row['ATM']-(1000*(j+1)))) + '.IV'
                elif symbol == 'BO':
                    salto_put = row['TickerSymbol'] + 'P' + str(int(row['ATM']-(500*(j+1)))) + '.IV'
                else:
                    salto_put = row['TickerSymbol'] + 'P' + str(int(row['ATM']-(100*(j+1)))) + '.IV'
                day_calls_puts.append(salto_put)
            calls_puts.append(day_calls_puts)
        print(calls_puts)
        iv_data = pd.DataFrame(columns=headers_iv_5n)
        for j in range(len(calls_puts)):
            time.sleep(2)
            url = base_url_prophetex + history_method
            params = urllib.parse.urlencode(
                {
                    'UserID':'ws@mfgrains.com',
                    'Password': 'Kr5o8N',
                    'Symbol': ','.join(calls_puts[j][4:]),
                    'StartDate': calls_puts[j][0],
                    'EndDate': calls_puts[j][0],
                    'Limit': 8000,
                    #'Market': 'CBOT', #market,
                    'Format': 'CSV'
                }
            )
            req = urllib.request.urlopen(url + '?%s' % params)
            print(req.geturl())
            try:
                content = req.read()
                df = pd.read_csv(StringIO(content.decode()), header=0)
                df = df.pivot(index='Date', columns='TickerSymbol', values='Close')            
                for index, row in df.iterrows():
                    #print(row)
                    iv_row = pd.DataFrame(
                        {
                            'Date': calls_puts[j][0],
                            'ExpirationSymbol': '@'+symbol+'@'+str(i+1),
                            'SymbolATM': calls_puts[j][1],
                            'Close': calls_puts[j][2],
                            'PriceATM': calls_puts[j][3],
                            'Symbol50D_C': calls_puts[j][4],
                            'Price50D_C': row[5],
                            'Symbol40D_C': calls_puts[j][5],
                            'Price40D_C': row[4],
                            'Symbol30D_C': calls_puts[j][6],
                            'Price30D_C': row[3],
                            'Symbol20D_C': calls_puts[j][7],
                            'Price20D_C': row[2],
                            'Symbol10D_C': calls_puts[j][8],
                            'Price10D_C': row[1],
                            'SymbolATM_C': calls_puts[j][9],
                            'PriceATM_C': row[0],
                            'SymbolATM_P': calls_puts[j][10],
                            'PriceATM_P': row[11],
                            'Symbol_10D_P': calls_puts[j][11],
                            'Price_10D_P': row[10],
                            'Symbol_20D_P': calls_puts[j][12],
                            'Price_20D_P': row[9],
                            'Symbol_30D_P': calls_puts[j][13],
                            'Price_30D_P': row[8],
                            'Symbol_40D_P': calls_puts[j][14],
                            'Price_40D_P': row[7],
                            'Symbol_50D_P': calls_puts[j][15],
                            'Price_50D_P': row[6]
                        }, index=[0]
                    )
                    iv_data = iv_data.append(iv_row, ignore_index=True)
            except:
                print('Except')
        if not iv_data.empty:
            iv = iv.append(iv_data, ignore_index=True)
            #iv['Skew']=iv[['Price50D_C','Price40D_C','Price30D_C','Price20D_C','Price10D_C','PriceATM_C','PriceATM_P','Price_10D_P','Price_20D_P','Price_30D_P','Price_40D_P','Price_50D_P']].skew(axis = 1, skipna = True)         
            iv['call']=iv[['Price50D_C','Price40D_C','Price30D_C','Price20D_C','Price10D_C','PriceATM_C']].sum(axis = 1, skipna = True)                 
            iv['put']=iv[['PriceATM_P','Price_10D_P','Price_20D_P','Price_30D_P','Price_40D_P','Price_50D_P']].sum(axis = 1, skipna = True)                 
            iv['Skew']=iv['call']-iv['put']
    return iv

def COMMODITIES_VI_5N():    
    #date_today = datetime.date.today().strftime(date_format)
    records = get_last_record_date(sql_iv_next)
    data = pd.DataFrame(columns=headers_iv_5n)
    for i in range(len(records)):
        commodity = records[i][0]
        last_date = records[i][1]
        data_iv_next_n_expirations = get_volatilidad_implicita_next_expirations(5,commodity,last_date)                
        data = data.append(data_iv_next_n_expirations[headers_iv_5n])
    data['actualizacion'] = datetime.datetime.now()
    
    if data.empty:
        r = '{"Result":"False"}'
    else:
        r = '{"Result":"True"}'
        data = data.replace('---','0')    
    
    upload_azure(data, 'COMMODITIES_VI_5N')
    return r    
