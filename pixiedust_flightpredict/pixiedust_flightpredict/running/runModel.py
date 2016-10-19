# -------------------------------------------------------------------------------
# Copyright IBM Corp. 2016
# 
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# -------------------------------------------------------------------------------

from .flightAccess import *
from .weatherAccess import *
from .flightHistory import *
from pixiedust_flightpredict import Configuration
from pixiedust_flightpredict.training import *
from collections import Counter
import pixiedust
import json

myLogger = pixiedust.getLogger(__name__)

"""
{'appendix': {'airlines': [{'active': True,
    'fs': 'NK',
    'iata': 'NK',
    'icao': 'NKS',
    'name': 'Spirit Airlines',
    'phoneNumber': '800-772-7117'}],
  'airports': [{'active': True,
    'city': 'Boston',
    'cityCode': 'BOS',
    'classification': 1,
    'countryCode': 'US',
    'countryName': 'United States',
    'elevationFeet': 19,
    'faa': 'BOS',
    'fs': 'BOS',
    'iata': 'BOS',
    'icao': 'KBOS',
    'latitude': 42.36646,
    'localTime': '2016-10-03T23:57:34.128',
    'longitude': -71.020176,
    'name': 'Logan International Airport',
    'postalCode': '02128-2909',
    'regionName': 'North America',
    'stateCode': 'MA',
    'street1': 'One Harborside Drive',
    'street2': '',
    'timeZoneRegionName': 'America/New_York',
    'utcOffsetHours': -4.0,
    'weatherZone': 'MAZ015'},
   {'active': True,
    'city': 'Las Vegas',
    'cityCode': 'LAS',
    'classification': 1,
    'countryCode': 'US',
    'countryName': 'United States',
    'elevationFeet': 2095,
    'faa': 'LAS',
    'fs': 'LAS',
    'iata': 'LAS',
    'icao': 'KLAS',
    'latitude': 36.081,
    'localTime': '2016-10-03T20:57:34.128',
    'longitude': -115.147599,
    'name': 'McCarran International Airport',
    'postalCode': '89119',
    'regionName': 'North America',
    'stateCode': 'NV',
    'street1': '5757 Wayne Newton Boulevard',
    'timeZoneRegionName': 'America/Los_Angeles',
    'utcOffsetHours': -7.0,
    'weatherZone': 'NVZ020'}],
  'equipments': [{'iata': '32S',
    'jet': True,
    'name': 'Airbus A318 / A319 / A320 / A321',
    'regional': False,
    'turboProp': False,
    'widebody': False}]},
 'request': {'carrier': {'fsCode': 'NK', 'requestedCode': 'NK'},
  'codeType': {},
  'date': {'day': '12',
   'interpreted': '2016-10-12',
   'month': '10',
   'year': '2016'},
  'departing': True,
  'flightNumber': {'interpreted': '641', 'requested': '641'},
  'url': 'https://api.flightstats.com/flex/schedules/rest/v1/json/flight/NK/641/departing/2016/10/12'},
 'scheduledFlights': [{'arrivalAirportFsCode': 'LAS',
   'arrivalTerminal': '1',
   'arrivalTime': '2016-10-12T19:34:00.000',
   'carrierFsCode': 'NK',
   'codeshares': [],
   'departureAirportFsCode': 'BOS',
   'departureTerminal': 'B',
   'departureTime': '2016-10-12T16:40:00.000',
   'flightEquipmentIataCode': '32S',
   'flightNumber': '641',
   'isCodeshare': False,
   'isWetlease': False,
   'referenceCode': '1874-588916--',
   'serviceClasses': ['R', 'Y'],
   'serviceType': 'J',
   'stops': 0,
   'trafficRestrictions': []}]}
"""

def runModel(flight, date, departureAirport=None):
    if not Configuration.isReadyForRun():
        raise ValueError("Unable to predict flight delay because no models are available. Please run configure for more details")

    if departureAirport is None:
        departureAirport = "LAS"

    response = getFlightSchedule(flight, date)
    myLogger.debug("Schedule for flight {0} at date {1} : {2}".format(flight, date, response ))
    if "error" in response:
        return {"error": "Unable to access schedule {0}".format(response["error"])}

    payload={}
    appendix=response['appendix']
    payload["flightInfo"]=scheduledFlight=response["scheduledFlight"]

    def getAirportJSON(code):
        for airport in appendix["airports"]:
            if airport['fs'] == code:
                return airport
    
    payload["departureAirportInfo"]=departureInfo={}
    departureInfo["airportInfo"]=depAirportJSON=getAirportJSON( scheduledFlight['departureAirportFsCode'] )
    departureInfo["weatherForecast"]= depWeather = getWeather(depAirportJSON['latitude'], depAirportJSON['longitude'], scheduledFlight["departureTimeUTC"])

    payload["arrivalAirportInfo"]=arrivalInfo={}
    arrivalInfo["airportInfo"]=arrAirportJSON=getAirportJSON( scheduledFlight['arrivalAirportFsCode'] )
    arrivalInfo["weatherForecast"]= arrWeather=getWeather(arrAirportJSON['latitude'], arrAirportJSON['longitude'], scheduledFlight["arrivalTimeUTC"] )

    payload["prediction"]=prediction = {}
    prediction["models"]=predictionModels = []
    #create the features vector
    features = createFeaturesVector(depWeather, arrWeather, scheduledFlight, depAirportJSON, arrAirportJSON)
    #count the predicted classes to make an overall prediction
    predictedClasses = []
    trainingHandler = getTrainingHandler();
    for modelVar,model in Configuration.getModels():
        p = model.predict(features)
        predictedClasses.append(p)
        predictionModels.append({
            "model": model.__class__.__name__,
            "prediction": trainingHandler.getClassLabel(p)
        })
    classes = Counter(predictedClasses).most_common()
    if len(classes)>1 and classes[0][1]==classes[1][1]:
        prediction["overall"] = "Undecided: {0}/{1}".format(trainingHandler.getClassLabel(classes[0][0]), trainingHandler.getClassLabel(classes[1][0]))
    else:
        prediction["overall"] = trainingHandler.getClassLabel(classes[0][0])
    """
    payload["prediction"]={
        "overall": "Delayed between 13 and 41 minutes",
        "models":[
            {"model":"NaiveBayesModel", "prediction":"Delayed between 13 and 41 minutes"},
            {"model":"DecisionTreeModel: Delayed between 13 and 41 minutes"},
            {"model":"LogisticRegressionModel: Delayed between 13 and 41 minutes"},
            {"model":"RandomForestModel: Delayed between 13 and 41 minutes"}
        ]
    }
    """

    myLogger.debug("runModel Payload {0}".format(payload))
    try:
        saveFlightResults(payload)
    except:
        myLogger.exception("Unable to save flight results {0}".format(payload))

    return json.dumps(payload)
    #payload format
    """
    {
        "flightInfo": <flightInfo>,
        "departureAirportInfo":{
            "airportInfo": <airport>
            "weatherForecast": <weather>
        },
        "arrivalAirportInfo":{
            "airportInfo": <airport>
            "weatherForecast": <weather>
        },
        "prediction": {
            "overall": <prediction>,
            "models":[
                { 
                    "model": <modelName>
                    "prediction": <prediction>
                },
                ...
            ]
        }
    }
    """

def mapAttribute(attr):
    if attr=="dewPt":
        return "dewpt"
    return attr

def createFeaturesVector(depWeather, arrWeather, scheduledFlight, arrAirport, depAirport):
    features=[]
    for attr in attributes:
        features.append(depWeather[mapAttribute(attr)])
    for attr in attributes:
        features.append(arrWeather[mapAttribute(attr)])
    
    #Call training handler for custom features
    s=type('dummy', (object,), {
        'departureTime':scheduledFlight["departureTime"], 'arrivalTime':scheduledFlight["arrivalTime"], 'arrivalAirportFsCode': arrAirport["fs"], 
        'departureAirportFsCode':depAirport["fs"],'departureWeather': depWeather, 'arrivalWeather': arrWeather})

    myLogger.debug("Creating features vector with {0}".format(s))
    for value in getTrainingHandler().customTrainingFeatures(s):
        features.append( value )
    
    return features

def runModelTest(flight, date, departureAirport=None):
    payload = {
        'arrivalAirportInfo': {
            'airportInfo': {
                'active': True,
                'city': 'Las Vegas',
                'cityCode': 'LAS',
                'classification': 1,
                'countryCode': 'US',
                'countryName': 'United States',
                'elevationFeet': 2095,
                'faa': 'LAS',
                'fs': 'LAS',
                'iata': 'LAS',
                'icao': 'KLAS',
                'latitude': 36.081,
                'localTime': '2016-10-04T12:28:48.884',
                'longitude': -115.147599,
                'name': 'McCarran International Airport',
                'postalCode': '89119',
                'regionName': 'North America',
                'stateCode': 'NV',
                'street1': '5757 Wayne Newton Boulevard',
                'timeZoneRegionName': 'America/Los_Angeles',
                'utcOffsetHours': -7.0,
                'weatherZone': 'NVZ020'
            },
            'weatherForecast': {
                'class': 'fod_short_range_hourly',
                'clds': 0,
                'day_ind': 'D',
                'dewpt': -9,
                'dow': 'Tuesday',
                'expire_time_gmt': 1475609398,
                'fcst_valid': 1475622000,
                'fcst_valid_local': '2016-10-04T16:00:00-0700',
                'feels_like': 26,
                'golf_category': 'Very Good',
                'golf_index': 9,
                'gust': None,
                'hi': 26,
                'icon_code': 32,
                'icon_extd': 3200,
                'mslp': 1006.5,
                'num': 4,
                'phrase_12char': 'Sunny',
                'phrase_22char': 'Sunny',
                'phrase_32char': 'Sunny',
                'pop': 0,
                'precip_type': 'rain',
                'qpf': 0.0,
                'rh': 9,
                'severity': 1,
                'snow_qpf': 0.0,
                'subphrase_pt1': 'Sunny',
                'subphrase_pt2': '',
                'subphrase_pt3': '',
                'temp': 26,
                'uv_desc': 'Low',
                'uv_index': 2,
                'uv_index_raw': 1.7,
                'uv_warning': 0,
                'vis': 16.0,
                'wc': 26,
                'wdir': 112,
                'wdir_cardinal': 'ESE',
                'wspd': 8,
                'wxman': 'wx1000'
            }
        },
        'departureAirportInfo': {
            'airportInfo': {
                'active': True,
                'city': 'Boston',
                'cityCode': 'BOS',
                'classification': 1,
                'countryCode': 'US',
                'countryName': 'United States',
                'elevationFeet': 19,
                'faa': 'BOS',
                'fs': 'BOS',
                'iata': 'BOS',
                'icao': 'KBOS',
                'latitude': 42.36646,
                'localTime': '2016-10-04T15:28:48.884',
                'longitude': -71.020176,
                'name': 'Logan International Airport',
                'postalCode': '02128-2909',
                'regionName': 'North America',
                'stateCode': 'MA',
                'street1': 'One Harborside Drive',
                'street2': '',
                'timeZoneRegionName': 'America/New_York',
                'utcOffsetHours': -4.0,
                'weatherZone': 'MAZ015'
            },
            'weatherForecast': {
                'class': 'fod_short_range_hourly',
                'clds': 29,
                'day_ind': 'D',
                'dewpt': 10,
                'dow': 'Tuesday',
                'expire_time_gmt': 1475609399,
                'fcst_valid': 1475611200,
                'fcst_valid_local': '2016-10-04T16:00:00-0400',
                'feels_like': 14,
                'golf_category': 'Very Good',
                'golf_index': 8,
                'gust': None,
                'hi': 15,
                'icon_code': 34,
                'icon_extd': 3400,
                'mslp': 1027.1,
                'num': 1,
                'phrase_12char': 'M Sunny',
                'phrase_22char': 'Mostly Sunny',
                'phrase_32char': 'Mostly Sunny',
                'pop': 0,
                'precip_type': 'rain',
                'qpf': 0.0,
                'rh': 69,
                'severity': 1,
                'snow_qpf': 0.0,
                'subphrase_pt1': 'Mostly',
                'subphrase_pt2': 'Sunny',
                'subphrase_pt3': '',
                'temp': 15,
                'uv_desc': 'Low',
                'uv_index': 1,
                'uv_index_raw': 1.31,
                'uv_warning': 0,
                'vis': 16.0,
                'wc': 14,
                'wdir': 54,
                'wdir_cardinal': 'NE',
                'wspd': 24,
                'wxman': 'wx1000'
            }
        },
        'flightInfo': {
            'arrivalAirportFsCode': 'LAS',
            'arrivalTerminal': '1',
            'arrivalTime': '2016-10-12T19:34:00.000',
            'carrierFsCode': 'NK',
            'codeshares': [],
            'departureAirportFsCode': 'BOS',
            'departureTerminal': 'B',
            'departureTime': '2016-10-12T16:40:00.000',
            'flightEquipmentIataCode': '32S',
            'flightNumber': '641',
            'isCodeshare': False,
            'isWetlease': False,
            'referenceCode': '1875-576194--',
            'serviceClasses': ['R', 'Y'],
            'serviceType': 'J',
            'stops': 0,
            'trafficRestrictions': []
        },
        'prediction': {
            'models': [
                {'model': 'NaiveBayesModel', 'prediction': 'Delayed between 13 and 41 minutes'},
                {'model': 'DecisionTreeModel', 'prediction': 'Delayed between 13 and 41 minutes'},
                {'model': 'LogisticRegressionModel', 'prediction': 'Delayed between 13 and 41 minutes'},
                {'model': 'RandomForestModel', 'prediction': 'Delayed between 13 and 41 minutes'}
            ],
            'overall': 'Delayed between 13 and 41 minutes'
        }
    }

    saveFlightResults(payload)
    return json.dumps(payload)

def runFlightSearch(date, time, departureAirport=None):
    if departureAirport is None:
        departureAirport = "LAS"
    
    myLogger.debug("Calling runFlightSearch with date:{0}, time:{1}, departureAirport:{2}".format(date, time, departureAirport))
    flights = getFlights(departureAirport, date, time )

    def getAirportName(code):
        for airport in flights['airports']:
            if airport['fs'] == code:
                return airport['name'] + ", " + airport['stateCode'] if 'stateCode' in airport else airport['countryName']

    payload = {'flights':[]}

    for scheduledFlight in flights['scheduledFlights']:
        flight={}
        flight['flightnumber'] = scheduledFlight['carrierFsCode'] + ' ' + scheduledFlight['flightNumber']
        flight['time'] = scheduledFlight['departureTime']
        flight['timeUTC'] = scheduledFlight['departureTimeUTC']
        flight['destAirportCode'] = scheduledFlight['arrivalAirportFsCode']
        flight['destination'] = getAirportName(scheduledFlight['arrivalAirportFsCode'])
        payload['flights'].append(flight)
    #payload = {
    #    'flights': [
    #        {'flightnumber': 'CVC 0811', 'time': '6:15pm', 'destination': 'Newark, NY'},
    #        {'flightnumber': 'VA 0315', 'time': '6:30pm', 'destination': 'Boston, MA'} 
    #    ]
    #}
    return json.dumps(payload)
