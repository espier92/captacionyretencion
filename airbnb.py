import scrapy
import re
import time
import datetime
from dateutil.relativedelta import relativedelta
import os
import wmi
from subprocess import Popen
import unicodecsv as csv
import requests
import boto3
import json
from base64 import b64encode

from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher

# Librerias para mandar emails
import smtplib
from email.mime.text import MIMEText

import psycopg2
import math
import random
from sshtunnel import SSHTunnelForwarder

import sys

import psycopg2

class AuthorSpider(scrapy.Spider):
# Nombre del crawler, a introducir en Anaconda prompt para ejecutarlo: scrapy crawl airbnbDB
    name = 'airbnbDB'
    errorcustfilename = 'logErrorCust.txt'
    ipfilename = 'ipUsed.txt'
    
    # Numero de instancias por proxy: 3 (el numero de instancias por region esta limitado a 2 en el config de scrapoxy, por lo que en cada proxy tendremos 2 instancias de una region y 1 instancia de otra region levantadas a la vez)
    numOfAWSInstances = 3
    # Puertos utilizados por cada proxy (en total levantamos 6 proxies)
    scrapoxyArray=["8888","8890","8892","8894","8896","8898"]
    # Puertos utilizados para administrar cada proxy a traves de su API
    scrapoxyApi=["8889","8891","8893","8895","8897","8899"]
    # Indice por el cual sabremos que proxy esta siendo usado (del 0 al 5)
    scrapoxyIdUsed=0
    scrapoxyTotalUsed=6
    # Cada vez que salten 1 captcha en un proxy, pasamos al siguiente del array
    # Cada vez que salten 4 captchas para un mismo proxy, resetearemos las maquinas de dicho proxy (no lo hacemos cada vez que salta el proxy ya que pese a haber saltado para una de las maquinas de ese proxy, las otras dos pueden seguir funcionando)
    maxIterations=4
    # Para que no se reseteen todos los proxies a la vez, los ponemos escalonados. Es decir, si saltan 4 capthas del proxy 1 lo reseteamos, pero los siguientes proxies no se resetearan ya que llevaran un captcha menos
    iterationsProxy=[3,2,1,0,-1,-2]
    # Contamos los anuncios visitados ya que cada aproximadamente 6 horas (20000 anuncios en idealista) necesitamos resetear las consolas de los proxies para que no den un out of memory
    contadorAnuncios=1
    cambioIterations=20000
    numAnunciosJSON=100
    countingCaptchas=0
    
    # Lo primero que haremos sera visitar una pagina en la que comprobaremos que la IP externa que estamos utilizando no es la de esta maquina de Azure, sino la de alguna maquina de los proxies.
    start_urls = [
        'https://www.google.com'
    ]
    # Leemos el fichero en el que esta la informacion a utilizar para explotar la API del portal inmobiliario
    setOfRegions = open("C:\\Users\\portal\\Documents\\scrapy\\airbnb\\listaDeBarrios.txt").readlines()
    # setOfRegions = open("C:\\Users\\portal\\Documents\\scrapy\\airbnb\\todasURLS.txt").readlines()
    
    # Flags para poner cabeceras en archivos de resultados y logs
    firstTime=True
    firstTimeError=True
    firstTimeCaptcha=True
    
    # Configuracion de donde y con que nombre se van a guardar los archivos de logs. Se les pone la fecha para que sean unicos, y sean mas faciles de identificar
    tsPath = time.time()
    stPath = datetime.datetime.fromtimestamp(tsPath).strftime('%Y-%m-%d_%H-%M-%S')
    script_dir = "C:\\Users\\portal\\Documents\\scrapy\\airbnb\\airbnb\\spiders"
    rel_path = "results\\items"+stPath+".csv"
    rel_error_path = "results\\notfound\\items"+stPath+".csv"
    rel_JSON_error_path = "results\\notfound\\jsonError"+stPath+".csv"
    rel_captcha_path = "results\\captcha\\items"+stPath+".csv"
    abs_file_path = os.path.join(script_dir, rel_path)
    abs_error_file_path = os.path.join(script_dir, rel_error_path)
    abs_JSON_error_file_path = os.path.join(script_dir, rel_JSON_error_path)
    abs_captcha_file_path = os.path.join(script_dir, rel_captcha_path)
    
    # Variables globales de las URLs a visitar
    parseURL=''
    parseIPURL=''
    parseIdURL=''
    parseIdNextURL=''
    callingURL=''
    tsRestart=''
    stRestart=''
    hoursToRestart=8
    tsRedimension=''
    stRedimension=''
    minsToRedimension=15
    startProxies=3
    ptgProxy1=0.01
    ptgProxy2=0.04
    requestOffset=50
    
    # codigos http de error que controlamos en el codigo del crawler (sino lo hicieramos el crawler se cerraria al encontrarse con alguno de ellos)
    http_status_codes = [ 400, 401, 402, 403, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417, 500, 501, 501, 503, 504, 505]
    
    # Flag y variables globales sobre la conexion a la base de datos local
    conn = None
    connFlag = False
    cursor=None
    
    # Variable global usada como apoyo para construir el json que sera enviado al servicio de testigos
    jsonAnuncios = []
    
    # Diccionarios para los tipos de via y para parsear fechas
    keyDic = ['id', 'promotionId', 'typeId','subtypeId','isNew','features','transactions','address','date','description','detail','advertiser']
    viaDic = ['c/','c','c.','calle','av','av.','av/','avda','avda.','avenida','paseo','pso','pso.','plaza','pza','pza.','vía','via','callejon','callejón','camino','carretera','ctra','ctra.','pasaje']
    viaAyudaDic = ['CALLE','CALLE','CALLE','CALLE','AVENIDA','AVENIDA','AVENIDA','AVENIDA','AVENIDA','AVENIDA','PASEO','PASEO','PASEO','PLAZA','PLAZA','PLAZA','VÍA','VÍA','CALLEJÓN','CALLEJÓN','CAMINO','CARRETERA','CARRETERA','CARRETERA','PASAJE']
    
    opDic = ['BUY','RENT']
    unitTypeDic = ['FLAT','HOUSE','LAND','PREMISES','STORAGE','GARAGE','NAVE','BUSINESS','OFFICE','BUILDING']
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        dispatcher.connect(self.quit_spider, signals.spider_closed)
        
        


        
        self.restartProxyNodes()
        
        self.tsRestart = time.time()
        self.stRestart = datetime.datetime.fromtimestamp(self.tsRestart)
        
        self.tsRedimension = time.time()
        self.stRedimension = datetime.datetime.fromtimestamp(self.tsRedimension)
        
        

            
        
    def quit_spider(self, spider):
        # Esta funcion se llamara cuando acabe la ejecucion del crawler (cuando haya visitado todas las urls deseadas)
        # Se cierra la conexion con la base de datos local y se hace commit de todos los cambios introducidos durante la ejecucion del crawler
        print('Closing DB Connection')
        if self.conn is not None:
            self.conn.commit()
            self.conn.close()
        # Si quedan anuncios por mandar a traves del servicio de testigos, los mandamos 
        if len(self.jsonAnuncios) > 0:
            if len(self.jsonAnuncios) < self.numAnunciosJSON:
                print('Llamada a servicio con jsonAnuncios[0:]')
                self.sendJSONtoWS(self.jsonAnuncios[0:])
            elif len(self.jsonAnuncios) == self.numAnunciosJSON:
                print('Llamada a servicio con jsonAnuncios[0:'+str(self.numAnunciosJSON-1)+']')
                self.sendJSONtoWS(self.jsonAnuncios[0:self.numAnunciosJSON-1])
            else:
                llamadas=math.floor(len(self.jsonAnuncios)/self.numAnunciosJSON)+1
                for x in range(0,llamadas):
                    if x == llamadas-1:
                        print('Llamada a servicio con jsonAnuncios[x*'+str(self.numAnunciosJSON)+':]')
                        self.sendJSONtoWS(self.jsonAnuncios[x*self.numAnunciosJSON:])
                    else:
                        print('Llamada a servicio con jsonAnuncios[x*'+str(self.numAnunciosJSON)+':(x+1)*'+str(self.numAnunciosJSON)+'-1]')
                        self.sendJSONtoWS(self.jsonAnuncios[x*self.numAnunciosJSON:(x+1)*self.numAnunciosJSON-1])
            self.jsonAnuncios = []
        
        print('----------------------------------------------')
        print('----------Spider finished its job-------------')
        print('----------------------------------------------')
                
                
        
        
        # Se actualiza el numero de instancias por proxy a 1, para así ahorrar costes
        for proxyScale in self.scrapoxyApi:
            urlScaling='http://localhost:'+proxyScale+'/api/scaling'
            dataScaling = {"min" : "1","required" : "1","max" : "1"}
            dataScaling_json = json.dumps(dataScaling)
            userAndPass = ""
            headers = { 'Authorization' : '%s' %  userAndPass , 'Accept' : 'application/json, text/plain' , 'Accept-Encoding' : 'gzip, deflate, br', 'Content-Type' : 'application/json;charset=UTF-8'}
            print('PATCH: Scaling Request')
            responseJSONScaling = requests.patch(urlScaling, data=dataScaling_json, headers=headers)
        
            if responseJSONScaling:
                print('-----------------------------------------------')
                print('Scaling response: ')
                print(responseJSONScaling)
                print('-----------------------------------------------')
            else:
                print('Error in scaling request')
            
    def parse(self, response):
        # Este metodo se llama cuando el crawler visita el start_url indicado al principio del codigo (en este caso no estamos haciendo nada aun, solo verificando que no hemos obtenido ningun codigo html de error)
        if response.status in self.http_status_codes:
            self.stopInstance(response)
        else:
            # Mandamos visitar la pagina para comprobar la IP del crawler en la funcion parseIP
            self.callingURL='https://www.my-ip-address.co/'
            self.parseURL=self.callingURL
            yield scrapy.Request(self.callingURL, callback=self.parseIP, meta={'proxy': '127.0.0.1:'+self.scrapoxyArray[self.scrapoxyIdUsed]}, errback=self.stopInstance)
        print('Exiting parse method')
            
    
    def parseIP(self, response):
        # Comprobamos que la IP externa con la que hemos visitado la pagina no es la de esta maquina, sino otra (la del proxy)
        if response.css('div.ipaddresstext h3::text'):
            print('IP address of request: '+response.css('div.ipaddresstext h3::text').extract_first())
            if not re.search('',response.css('div.ipaddresstext h3::text').extract_first()):
                # Ahora comprobamos que las peticiones GET tampoco se hacen con la IP de azure
                # Configuramos el uso del proxy para las peticiones
                http_proxy  = "http://127.0.0.1:"+self.scrapoxyArray[self.scrapoxyIdUsed]
                https_proxy = "https://127.0.0.1:"+self.scrapoxyArray[self.scrapoxyIdUsed]
                ftp_proxy   = "ftp://127.0.0.1:"+self.scrapoxyArray[self.scrapoxyIdUsed]

                proxyDict = { 
                              "http"  : http_proxy, 
                              "https" : https_proxy, 
                              "ftp"   : ftp_proxy
                            }

                r = requests.get('https://api.ipify.org', proxies=proxyDict)
                print('My public IP address is:'+r.text)
                if not re.search('',r.text) and r.status_code == 200:
                    # Si en efecto se esta utilizando correctamente las IPs del proxy, empezamos a explotar la API del portal inmobiliario
                    self.sendRequestsToAPI()
        else:
            print('Error in page')
            
        print('Exiting parseIP method')
    
    def sendRequestsToAPI(self):
        response=''
        # Recorremos todas las zonas geográficas contempladas en el archivo local (definidas por su query)
        for regionToVisit in self.setOfRegions:
            regionToVisit=re.sub(r'\r','',regionToVisit)
            regionToVisit=re.sub(r'\n','',regionToVisit)
            regionToVisit=re.sub(r'\s','%2C%20',regionToVisit)
            regionToVisit=re.sub(r'ñ','%C3%B1',regionToVisit)
            regionToVisit=re.sub(r'á','a',regionToVisit)
            regionToVisit=re.sub(r'é','e',regionToVisit)
            regionToVisit=re.sub(r'í','i',regionToVisit)
            regionToVisit=re.sub(r'ó','o',regionToVisit)
            regionToVisit=re.sub(r'ú','u',regionToVisit)
            pageNumber=1
            totalInmuebles=0
            counterInmuebles=0
            counterArray=[0,0,0,0,0,0,0,0]
            counterError=0
            keepInLoop=True
            # Cada vez que llamamos a la API, obtenemos informacion de cientos de anuncios de la region indicada. Tambien obtenemos el numero de anuncios total de la region, y llevamos la cuenta de cuantos hemos capturado
            # Por tanto no salimos del bucle hasta haber capturado todos, con una cierta tolerancia
            while keepInLoop:
                tsCompare = time.time()
                stCompare = datetime.datetime.fromtimestamp(tsCompare)
                diffTimeRedimension = relativedelta(stCompare, self.stRedimension)
                if diffTimeRedimension:
                    if diffTimeRedimension.minutes:
                        if diffTimeRedimension.minutes >= self.minsToRedimension:
                            print("Checking performance to see if we need to redimensionate the proxies")
                            captchaProp = 100*self.countingCaptchas/self.contadorAnuncios
                            waitLonger=False
                            if captchaProp < self.ptgProxy1:
                                self.startProxies=1
                            elif captchaProp < self.ptgProxy2:
                                if self.startProxies < 2:
                                    waitLonger=True
                                self.startProxies=2
                            else:
                                if self.startProxies < 3:
                                    waitLonger=True
                                self.startProxies=3
                            self.redimensionProxies(self.startProxies,waitLonger)
                            self.tsRedimension = time.time()
                            self.stRedimension = datetime.datetime.fromtimestamp(self.tsRedimension)
                diffTimeRestart = relativedelta(stCompare, self.stRestart)
                if diffTimeRestart:
                    if diffTimeRestart.hours:
                        if diffTimeRestart.hours >= self.hoursToRestart:
                            print('Restart time has arrived. Previous restart/beggining of execution occured '+str(self.hoursToRestart)+' hours ago')
                            self.restartProxyNodes()
                            self.tsRestart = time.time()
                            self.stRestart = datetime.datetime.fromtimestamp(self.tsRestart)
                            # .strftime('%Y-%m-%d %H:%M:%S')
                            if self.conn is not None:
                                self.conn.commit()
                            
                # Configuramos el uso del proxy para las peticiones
                http_proxy  = "http://127.0.0.1:"+self.scrapoxyArray[self.scrapoxyIdUsed]
                https_proxy = "https://127.0.0.1:"+self.scrapoxyArray[self.scrapoxyIdUsed]
                ftp_proxy   = "ftp://127.0.0.1:"+self.scrapoxyArray[self.scrapoxyIdUsed]

                proxyDict = { 
                              "http"  : http_proxy, 
                              "https" : https_proxy, 
                              "ftp"   : ftp_proxy
                            }
                            
                headers = {'accept': 'application/json, text/javascript, */*; q=0.01', "accept-encoding":'gzip, deflate, br', "accept-language":'es-ES,es;q=0.9', "cache-control": 'no-cache','pragma':'no-cache', "user-agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36', 'x-requested-with':'XMLHttpRequest'}
                urlToVisit="https://www.airbnb.es/api/v2/explore_tabs?version=1.4.5&satori_version=1.1.6&_format=for_explore_search_web&experiences_per_grid=20&guidebooks_per_grid=20&auto_ib=true&fetch_filters=true&has_zero_guest_treatment=true&is_guided_search=true&is_new_cards_experiment=true&luxury_pre_launch=true&query_understanding_enabled=true&show_groupings=true&supports_for_you_v3=true&timezone_offset=60&metadata_only=false&is_standard_search=true&refinement_paths%5B%5D=%2Fhomes&selected_tab_id=home_tab&checkin=&checkout=&adults=0&children=0&infants=0&guests=0&toddlers=0&title_type=MAGAZINE_HOMES&last_search_session_id=89f49ec1-c730-479d-9516-a09a39cf1de8&screen_size=large&_intents=p1&key=d306zoyjsyarp7ifhu67rjxn52tv0t20&currency=EUR&locale=es&query="+regionToVisit+"&items_per_grid="+str(requestOffset)+"&locale=es&items_offset="+str(counterInmuebles)
                print(urlToVisit)
                counterInmuebles=counterInmuebles+self.requestOffset
                r = requests.get(urlToVisit, headers=headers, proxies=proxyDict)
                if r:
                    counterError=0
                    print('Request code is:'+str(r.status_code))
                    if r.status_code is 200:
                        values = json.loads(r.text)
                        for tab in values['explore_tabs']:
                            # Extraemos toda la informacion del inmueble contenida en el JSON de respuesta de la API del portal
                            for key, value in tab.items():
                                if isinstance(value, (dict)):
                                    for key1, value1 in value.items():
                                        # pagination
                                        if re.search('pagination_metadata',key):
                                            if re.search('has_next_page',key1):
                                                print('KEEP IN LOOP: ')
                                                print(value1)
                                                if value1:
                                                    keepInLoop=True
                                                    print('is true')
                                                else:
                                                    keepInLoop=False
                                                    print('is false')
                                        
                                else:
                                    if re.search('sections',key):
                                         for section in value:
                                            for key2, value2 in section.items():
                                                if re.search('listings',key2):
                                                    for listing in value2:         
                                                        baths=''
                                                        sharedBaths=''
                                                        habs=''
                                                        beds=''
                                                        id=''
                                                        lat=''
                                                        lon=''
                                                        city=''
                                                        neighborhood=''
                                                        publicAddress=''
                                                        guests=''
                                                        personCapacity=''
                                                        previewAmenities=''
                                                        roomAndPropertyType=''
                                                        roomTypeCategory=''
                                                        roomType=''
                                                        spaceType=''
                                                        ownerPic=''
                                                        ownerName=''
                                                        ownerId=''
                                                        rateType=''
                                                        priceAmount=''
                                                        priceAmountWithServiceFee=''
                                                        weeklyPriceFactor=''
                                                        monthlyPriceFactor=''
                                                        
                                                        for key3, value3 in listing.items():
                                                            if re.search('listing',key3):
                                                                for key4, value4 in value3.items():
                                                                    if re.search('bathrooms',key4):
                                                                        baths=str(value4)
                                                                    elif re.search('bathroom_label',key4):
                                                                        sharedBaths=str(value4)
                                                                    elif re.search('bedrooms',key4):
                                                                        habs=str(value4)
                                                                    elif re.search('beds',key4):
                                                                        beds=str(value4)
                                                                    elif re.search('id',key4) and len(key4) is 2:
                                                                        id=str(value4)
                                                                    elif re.search('lat',key4):
                                                                        lat=re.sub(r'[\.]',',',str(value4))
                                                                    elif re.search('lng',key4):
                                                                        lon=re.sub(r'[\.]',',',str(value4))
                                                                    elif re.search('localized_city',key4):
                                                                        city=str(value4)
                                                                    elif re.search('localized_neighborhood',key4):
                                                                        neighborhood=str(value4)
                                                                    elif re.search('public_address',key4):
                                                                        publicAddress=str(value4)
                                                                    elif re.search('guest_label',key4):
                                                                        guests=str(value4)
                                                                    elif re.search('person_capacity',key4):
                                                                        personCapacity=str(value4)
                                                                    elif re.search('preview_amenities',key4):
                                                                        previewAmenities=str(value4)
                                                                    elif re.search('room_and_property_type',key4):
                                                                        roomAndPropertyType=str(value4)
                                                                    elif re.search('room_type_category',key4):
                                                                        roomTypeCategory=str(value4)
                                                                    elif re.search('room_type',key4):
                                                                        roomType=str(value4)
                                                                    elif re.search('space_type',key4):
                                                                        spaceType=str(value4)
                                                                    elif re.search('host_thumbnail_url',key4):
                                                                        ownerPic=re.sub('profile_x_medium','profile_x_large',str(value4))
                                                                    elif re.search('user',key4) and len(key4) is 4:
                                                                        for key5, value5 in value4.items():
                                                                            if re.search('first_name',key5):
                                                                                ownerName=str(value5)
                                                                            elif re.search('id',key5) and len(key5) is 2:
                                                                                ownerId=str(value5)
                                                            elif re.search('pricing_quote',key3):
                                                                for key6, value6 in value3.items():
                                                                    if re.search('rate_type',key6):
                                                                        rateType=str(value6)
                                                                    elif re.search('weekly_price_factor',key6):
                                                                        weeklyPriceFactor=str(value6)
                                                                    elif re.search('monthly_price_factor',key6):
                                                                        monthlyPriceFactor=str(value6)
                                                                    elif re.search('rate',key6) and len(key6) is 4:
                                                                        for key7, value7 in value6.items():
                                                                            if re.search('amount',key7) and len(key7) is 6:
                                                                                priceAmount=str(value7)
                                                                    elif re.search('rate_with_service_fee',key6):
                                                                        for key8, value8 in value6.items():
                                                                            if re.search('amount',key8) and len(key8) is 6:
                                                                                priceAmountWithServiceFee=str(value8)
                                                        print("---------------------------------------------++++++++++++++++++++++++++++++++++++++++++++++++--------------------------------------------")
                                                        print("---------------------------------------------+++++++++++++++UNIT CHARACTERISTICS+++++++++++++--------------------------------------------")
                                                        print('Baths: '+baths+'; sharedBaths: '+sharedBaths+'; habs: '+habs+'; beds: '+beds+'; id: '+id+'; lat: '+lat+'; lon: '+lon+'; city: '+city+'; neighborhood: '+neighborhood+'; publicAddress: '+publicAddress+'; guests: '+guests+'; personCapacity: '+personCapacity+'; previewAmenities: '+previewAmenities+'; roomAndPropertyType: '+roomAndPropertyType+'; roomTypeCategory: '+roomTypeCategory+'; roomType: '+roomType+'; spaceType: '+spaceType+'; ownerPic: '+ownerPic+'; ownerName: '+ownerName+'; ownerId: '+ownerId+'; rateType: '+rateType+'; priceAmount: '+priceAmount+'; priceAmountWithServiceFee: '+priceAmountWithServiceFee+'; weeklyPriceFactor: '+weeklyPriceFactor+'; monthlyPriceFactor: '+monthlyPriceFactor)
                                                        print("---------------------------------------------++++++++++++++++++++++++++++++++++++++++++++++++--------------------------------------------")
                                                        url="https://www.airbnb.es/rooms/"+id+"?"
                                                        ts = time.time()
                                                        st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
                                                        # Guardamos en fichero local los datos obtenidos del anuncio
                                                        csvfile = csv.writer(open(self.abs_file_path, 'ab'), dialect='excel', encoding='utf-8')
                                                        if self.firstTime:
                                                            csvfile.writerow(['Fuente', 'TIPO DE HAB Y PROPIEDAD','CATEGORIA DEL TIPO DE HABITACIÓN', 'TIPO DE HABITACIÓN', 'TIPO DE ESPACIO', 'CAPACIDAD','HUÉSPEDES','PREVISUALIZACION DE CARACTERISTICAS','HABITACIONES', 'CAMAS', 'BAÑOS', 'BAÑOS COMPARTIDOS', 'TIPO DE TARIFA','TARIFA','TARIFA CON SERVICIO','FACTOR DE DESCUENTO SEMANAL','FACTOR DE DESCUENTO MENSUAL','LATITUD', 'LONGITUD','BARRIO','CIUDAD','DIRECCION PÚBLICA','NOMBRE DEL DUEÑO','ID DEL DUEÑO','URL DE FOTO DEL DUEÑO','ID','URL' ])
                                                            self.firstTime=False
                                                        csvfile.writerow(['AIRBNB',roomAndPropertyType,roomTypeCategory,roomType,spaceType,personCapacity,guests,previewAmenities,habs,beds,baths,sharedBaths,rateType,priceAmount,priceAmountWithServiceFee,weeklyPriceFactor,monthlyPriceFactor,lat,lon,neighborhood,city,publicAddress,ownerName,ownerId,ownerPic,id,url ])
                else:
                    self.counterError=self.counterError+1
                    print('Error en la request')
                    self.stopInstance(response)
                    if self.counterError>5:
                        keepInLoop=False
                        
                print('-----------------------------------------------')
                print('START SLEEPING TIME')
                randomMin=random.uniform(2.68,4.33)
                randomMax=random.uniform(4.69,7.81)
                sleepTime=random.uniform(randomMin, randomMax)
                print('stop '+str(sleepTime)+' secs')
                time.sleep(sleepTime)
                print('DONE WITH THE NAP')
                print('-----------------------------------------------')
        
                    
                    
    
    def sendJSONtoWS(self, jsonToSend):
        # Esta funcion se encarga de mandar los anuncios incluidos en el JSON a traves del API expuesta para su explotacion
        print('Vamos a mandar el JSON via web service')
        print('Sending JSON: ')
        # Pasamos a String el JSON
        jsonStr="["
        for x in range(0,len(jsonToSend)):
            if x is not 0:
                jsonStr=jsonStr+','
            try:
                jsonToSend[x] = jsonToSend[x].decode('utf-8')
            except AttributeError:
                pass
            jsonStr=jsonStr+jsonToSend[x]
        jsonStr=jsonStr+"]"
        
        # responseJSON = requests.post(urlSend, json=json.loads(jsonStr), headers=headers)
        responseJSON = False
        # responseJSON = False
        if responseJSON:
            print('-----------------------------------------------')
            print('Sending JSON response: ')
            print(responseJSON)
            if responseJSON.status_code is 200:
                print('Success')
                # # En caso de exito, el servicio nos devuelve un porcentaje de 'eficiencia' (% de anuncios que se han insertado correctamente en base de datos) y el id de los que hayan fallado
                
            else:
                # Si el servicio no devuelve un OK guardamos el JSON en un fichero por si queremos revisarlo o mandarlo mas adelante
                # print(jsonStr)
                print('Error in method: '+responseJSON.status_code)
                with open(self.abs_JSON_error_file_path, "a", encoding='utf-8') as myfile:
                    myfile.write(jsonStr)
                    myfile.write("\n")
            print('-----------------------------------------------')
        else:
            # Si el servicio no devuelve un OK guardamos el JSON en un fichero por si queremos revisarlo o mandarlo mas adelante
            print('Error in JSONsending request')
            # print(jsonStr)
            with open(self.abs_JSON_error_file_path, "a", encoding='utf-8') as myfile:
                myfile.write(jsonStr)
                myfile.write("\n")
                
    def redimensionProxies(self,nrProxies,waitLonger):
        # Se actualiza el numero de instancias por proxy al numero indicado, en funcion del numero de captchas que ha habido durante el periodo de ejecucion
        waitRedimensionFlag=False
        for proxyScale in self.scrapoxyApi:
            urlScaling='http://localhost:'+proxyScale+'/api/scaling'
            dataScaling = {"min" : "1","required" : str(nrProxies),"max" : str(nrProxies)}
            dataScaling_json = json.dumps(dataScaling)
            userAndPass = ""
            headers = { 'Authorization' : '%s' %  userAndPass , 'Accept' : 'application/json, text/plain' , 'Accept-Encoding' : 'gzip, deflate, br', 'Content-Type' : 'application/json;charset=UTF-8'}
            print('PATCH: Scaling Request')
            responseJSONScaling = requests.patch(urlScaling, data=dataScaling_json, headers=headers)
        
            if responseJSONScaling:
                print('-----------------------------------------------')
                print('Scaling response: ')
                print(responseJSONScaling)
                print('-----------------------------------------------')
                if responseJSONScaling.status_code is 200:
                    waitRedimensionFlag=True
            else:
                print('Error in scaling request')
        if waitRedimensionFlag:
            # Esperamos a que acaben los proxies de cerrarse
            if waitLonger:
                sleepTime=40
                print('---------------------------------------------------------------------------------------')
                print('--------------------------------SUBIENDO NUMERO DE PROXIES-----------------------------')
                print('---------------------------------------------------------------------------------------')
            else:
                sleepTime=10
                print('---------------------------------------------------------------------------------------')
                print('--------------------------------BAJANDO NUMERO DE PROXIES-----------------------------')
                print('---------------------------------------------------------------------------------------')
            print(' Waiting for proxies: '+str(sleepTime)+' secs')
            time.sleep(sleepTime)
    
    def restartProxyNodes(self):
        # Este metodo se encarga de resetear los proxies para que asi no haya un out of memory
        print('Restarting proxy nodes')
        c = wmi.WMI ()
        # Nos cargamos los node.js de los proxies
        for process in c.Win32_Process ():
            if re.search('node.exe',process.Name):
                os.system("taskkill /F /t /pid "+str(process.ParentProcessId))
        # Nos cargamos los proxies
        for process in c.Win32_Process ():
            if re.search('cmd.exe',process.Name):
                if re.search('crapoxy',process.CommandLine):
                    os.system("taskkill /F /t /pid "+str(process.ProcessId))
        # Esperamos a que acaben los proxies de cerrarse
        sleepTime=5
        print(' Waiting for proxies to stop: '+str(sleepTime)+' secs')
        time.sleep(sleepTime)
        sleepTime=60
        # Lanzamos un script que lanza de nuevo los proxies y esperamos 1 minuto para que se acaben de levantar
        print(' Launching new proxies and waiting : '+str(sleepTime)+' secs')
        if self.startProxies == 1:
            p = Popen("C:\\Users\\portal\\Desktop\\Test\\Reinicio\\testReinicio_1.bat")
        elif self.startProxies == 2:
            p = Popen("C:\\Users\\portal\\Desktop\\Test\\Reinicio\\testReinicio_2.bat")
        else:
            p = Popen("C:\\Users\\portal\\Desktop\\Test\\Reinicio\\testReinicio.bat")
        time.sleep(sleepTime)
            
    def stopInstance(self, response):
        self.countingCaptchas=self.countingCaptchas+1
        # Aqui entramos cuando salta un captcha o se produce algun error inesperado
        print('Un captcha salvaje ha aparecido')        
        if self.iterationsProxy[self.scrapoxyIdUsed] > self.maxIterations:
            # Si hemos superado el numero de iteraciones minimas para resetear los proxies (4 normalmente), procedemos
            self.iterationsProxy[self.scrapoxyIdUsed]=0
            contador=0
            dnsName=[]
            ips=[]
            ids=[]
            region1=["us-east-1","ap-south-1"]
            region2=["ap-northeast-2","us-east-2"]
            region3=["ca-central-1","us-west-1"]
            region4=["eu-west-2","ap-southeast-2"]
            region5=["eu-west-3","ap-northeast-1"]
            region6=["us-west-2","ap-southeast-1"]
            regions=[region1,region2,region3,region4,region5,region6]
            regionNumber=len(regions[self.scrapoxyIdUsed])
            regionPos=[]
            client=[]
            responseAWS=[]
            # Guardamos la informacion de las instancias del proxy que queremos reiniciar (de las diferentes regiones que componen el proxy)
            print('Scrapoxy used: '+str(self.scrapoxyIdUsed))
            for reg in range(0, regionNumber): 
                client.append(boto3.client('ec2',
                    aws_access_key_id='',
                    aws_secret_access_key='',
                    region_name=regions[self.scrapoxyIdUsed][reg]
                ))
                responseAWS.append(client[reg].describe_instances())
            # Obtenemos las direcciones ips, ids y dns de las instancias que debemos detener
            for response in responseAWS:
                for group in response["Reservations"]:
                    for inst in group["Instances"]:
                        if re.search(r'running',inst["State"]["Name"]):
                            dnsName.append(inst["PublicDnsName"])
                            ips.append(inst["PublicIpAddress"])
                            ids.append(inst["InstanceId"])
                            contador=contador+1
                        
                regionPos.append(contador)
            
                  
            print('dnsName: ')
            print(dnsName)
            print('IPs: ')
            print(ips)
            print('ids: ')
            print(ids)
            print('contador: '+str(contador))
            hasSlept = False
            # Matamos las instancias del proxy a reiniciar
            for x in range(0, contador):
                print('Stopping instance: '+ids[x])            
                if regionNumber is 1:
                    responseTerminateInstance = client[0].terminate_instances(InstanceIds=[ids[x]])
                elif regionNumber is 2:
                    if x<regionPos[0]:
                        responseTerminateInstance = client[0].terminate_instances(InstanceIds=[ids[x]])
                    elif x<regionPos[1]:
                        responseTerminateInstance = client[1].terminate_instances(InstanceIds=[ids[x]])
                elif regionNumber is 3:
                    if x<regionPos[0]:
                        responseTerminateInstance = client[0].terminate_instances(InstanceIds=[ids[x]])
                    elif x<regionPos[1]:
                        responseTerminateInstance = client[1].terminate_instances(InstanceIds=[ids[x]])
                    elif x<regionPos[2]:
                        responseTerminateInstance = client[2].terminate_instances(InstanceIds=[ids[x]])
                elif regionNumber is 4:
                    if x<regionPos[0]:
                        responseTerminateInstance = client[0].terminate_instances(InstanceIds=[ids[x]])
                    elif x<regionPos[1]:
                        responseTerminateInstance = client[1].terminate_instances(InstanceIds=[ids[x]])
                    elif x<regionPos[2]:
                        responseTerminateInstance = client[2].terminate_instances(InstanceIds=[ids[x]])
                    elif x<regionPos[3]:
                        responseTerminateInstance = client[3].terminate_instances(InstanceIds=[ids[x]])
            # Cuando matas las instancias, scrapoxy se encarga automaticamente de levantar nuevas para cubrir el numero de instancias minimas configuradas (normalmente 3)
            # Esperamos 10 segundos a que se levanten las nuevas instancias. Realmente tardan mas en levantarse, pero como hay 5 proxies mas, para cuando le vuelva a tocar a este proxy ser utilizado ya se habran levantado las maquinas
            sleepTime=10
            print(' Waiting for new instances to be deployed: '+str(sleepTime)+' secs')
            time.sleep(sleepTime)
        else:
            # Si no hemos llegado al numero minimo de iteraciones para resetear el proxy, aumentamos el contador para ese proxy y seguimos con la ejecucion del crawler
            self.iterationsProxy[self.scrapoxyIdUsed]=self.iterationsProxy[self.scrapoxyIdUsed]+1
        # Independientemente de si hemos reseteado o no el proxy, cambiamos al proxy siguiente para seguir con la ejecucion (ya que ha saltado un captcha en el que estabamos)   
        print('Changing proxy. Current: '+ str(self.scrapoxyIdUsed))
        self.scrapoxyIdUsed = self.scrapoxyIdUsed + 1 if self.scrapoxyIdUsed < self.scrapoxyTotalUsed -1 else 0
        print('New proxy: '+ str(self.scrapoxyIdUsed))
        yield None