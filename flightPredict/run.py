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

import requests
from datetime import datetime
from dateutil import parser
from IPython.display import display, HTML

import training as f

mlModels=None
        
def formatWeather(weather):
    html = "<ul><li><b>Forecast:</b> " + weather['phrase_12char'] + '</li>'
    for attr,msg in zip(f.attributes, f.attributesMsg):
        html+="<li><b>"+msg+":</b> "+ str(weather[mapAttribute(attr)])
    html+="</ul>"
    return html
    
def getWeather(airportCode, dtString):
    import training as f
    dt=parser.parse(dtString)
    schema="" if (f.cloudantHost.startswith("http")) else "https://"
    url=schema + f.cloudantHost+'/flight-metadata/_design/flightMetadata/_view/US%20Airports?include_docs=true&keys=[%22'+airportCode+'%22]'
    response = requests.get(url,auth=(f.cloudantUserName, f.cloudantPassword))
    doc = response.json()['rows'][0]['doc']

    # weather insights
    #url=f.weatherUrl +'/api/weather/v2/forecast/hourly/24hour'
    #forecasts=requests.get(url, params=[('geocode',str(doc['latitude'])+','+str(doc['longitude'])),('units','m'),('language','en-US')]).json()['forecasts']

    # weather company data
    url=f.weatherUrl + '/api/weather/v1/geocode/{lat}/{long}/forecast/hourly/48hour.json'.format(lat=str(doc['latitude']),long=str(doc['longitude']))
    forecasts=requests.get(url, params=[('units','m'),('language','en-US')]).json()['forecasts']

    #find the forecasts that is closest to dt.tm_hour
    weatherForecast=None
    for f in forecasts:
        ftime=datetime.fromtimestamp(f['fcst_valid'])
        if ftime.hour==dt.hour:
            weatherForecast=f
    return (weatherForecast, doc['name'],str(doc['latitude']),str(doc['longitude']))

def useModels(*models):
    global mlModels
    mlModels=models

def mapAttribute(attr):
    if attr=="dewPt":
        return "dewpt"
    return attr
    
def runModel(depAirportCode, departureDT, arrAirportCode, arrivalDT):
    import training as f
    depTuple = getWeather(depAirportCode, departureDT)
    arrTuple = getWeather(arrAirportCode, arrivalDT)
    depWeather=depTuple[0]
    arrWeather=arrTuple[0]

    #create the features vector
    features=[]
    for attr in f.attributes:
        features.append(depWeather[mapAttribute(attr)])
    for attr in f.attributes:
        features.append(arrWeather[mapAttribute(attr)])
    
    #Call training handler for custom features
    s=type('dummy', (object,), {'departureTime':departureDT, 'arrivalTime':arrivalDT, 'arrivalAirportFsCode': arrAirportCode, 
                    'departureAirportFsCode':depAirportCode,'departureWeather': depWeather, 'arrivalWeather': arrWeather})
    customFeaturesForRunModel=f.getTrainingHandler().customTrainingFeatures(s) 
    for value in customFeaturesForRunModel:
        features.append( value )

    html='<table width=100%><tr><th>'+depTuple[1]+'</th><th>Prediction</th><th>'+arrTuple[1]+'</th></tr>'
    html+='<tr><td>'+formatWeather(depWeather)+'</td>'
    html+='<td><ul>'
    for model in mlModels:
        label= model.__class__.__name__
        html+='<li>' + label + ': ' + f.getTrainingHandler().getClassLabel(model.predict(features)) + '</li>'
    html+='</ul></td>'
    html+='<td>'+formatWeather(arrWeather)+'</td>'
    html+='</tr></table>'
    html+='<div id="map" style="height:300px"></div>'
    
    #display map
    html+='<script async defer src="https://maps.googleapis.com/maps/api/js?key=AIzaSyBBfYX6GG1foO1l7TAPk2LQVV_nACb7T4Q&callback=renderMap" type="text/javascript"></script>'
    html+='<script type="text/javascript">'
    html+='function renderMap() {'
    html+='var map = new google.maps.Map(document.getElementById("map"), {zoom: 4,center: new google.maps.LatLng(40, -100),mapTypeId: google.maps.MapTypeId.TERRAIN});'
    html+='var depAirport = new google.maps.LatLng(' + depTuple[2] + ',' + depTuple[3] + ');'
    html+='var arrAirport = new google.maps.LatLng(' + arrTuple[2] + ',' + arrTuple[3] + ');'
    html+='var markerP1 = new google.maps.Marker({position: depAirport, map: map});'
    html+='var markerP2 = new google.maps.Marker({position: arrAirport, map: map});'
    html+='var flightPlanCoordinates = [depAirport,arrAirport];'
    html+='var flightPath = new google.maps.Polyline({path: flightPlanCoordinates,strokeColor: "#0000FF",strokeOpacity: 1.0,strokeWeight: 2});'
    html+='flightPath.setMap(map);}'
    html+='</script>'

    display(HTML(html))
