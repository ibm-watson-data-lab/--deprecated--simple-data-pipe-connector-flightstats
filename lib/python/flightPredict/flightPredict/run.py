import requests
from datetime import datetime
from datetime import timezone
from IPython.display import display, HTML

from flightPredict import cloudantHost, cloudantUserName, cloudantPassword, weatherUrl

attributes=['dewpt','rh','vis','wc','wdir','wspd','feels_like','uv_index']
attributesMsg = ['Dew Point', 'Relative Humidity', 'Prevailing Hourly visibility', 'Wind Chill', 'Wind direction',\
                'Wind Speed','Feels Like Temperature', 'Hourly Maximum UV Index']
    
def formatWeather(weather):
    html = "<ul><li><b>Forecast:</b> " + weather['phrase_12char'] + '</li>'
    for attr,msg in zip(attributes, attributesMsg):
        html+="<li><b>"+msg+":</b> "+ str(weather[attr])
    html+="</ul>"
    return html

def formatPrediction(prediction):
    if ( prediction==0 ):
        return "Canceled"
    elif (prediction==1 ):
        return "On Time"
    elif (prediction == 2 ):
        return "Delayed less than 2 hours"
    elif (prediction == 3 ):
        return "Delayed between 2 and 4 hours"
    elif (prediction == 4 ):
        return "Delayed more than 4 hours"
    return prediction
    
def getWeather(airportCode, dtString):
    dt=datetime.strptime(dtString, "%Y-%m-%d %H:%M%z")
    dt=dt.astimezone(timezone.utc)
    url=cloudantHost+'/flight-metadata/_design/flightMetadata/_view/US%20Airports?include_docs=true&key=%22'+airportCode+'%22'
    response = requests.get(url,auth=(cloudantUserName, cloudantPassword))
    doc = response.json()['rows'][0]['doc']
    url=weatherUrl +'/api/weather/v2/forecast/hourly/24hour'
    forecasts=requests.get(url, params=[('geocode',str(doc['latitude'])+','+str(doc['longitude'])),('units','m'),('language','en-US')]).json()['forecasts']
    #find the forecasts that is closest to dt.tm_hour
    weatherForecast=None
    for f in forecasts:
        ftime=datetime.fromtimestamp(f['fcst_valid'])
        if ftime.hour==dt.hour:
            weatherForecast=f
    return (weatherForecast, doc['name'],str(doc['latitude']),str(doc['longitude']))

mlModels=None
def useModels(*models):
    global mlModels
    mlModels=models
    
def runModel(depAirportCode, departureDT, arrAirportCode, arrivalDT):
    depTuple = getWeather(depAirportCode, departureDT)
    arrTuple = getWeather(arrAirportCode, arrivalDT)
    depWeather=depTuple[0]
    arrWeather=arrTuple[0]

    #create the features vector
    features=[]
    for attr in attributes:
        features.append(depWeather[attr])
    for attr in attributes:
        features.append(arrWeather[attr])

    html='<table width=100%><tr><th>'+depTuple[1]+'</th><th>Prediction</th><th>'+arrTuple[1]+'</th></tr>'
    html+='<tr><td>'+formatWeather(depWeather)+'</td>'
    html+='<td><ul>'
    for model in mlModels:
        label= model.__class__.__name__
        html+='<li>' + label + ': ' + formatPrediction(model.predict(features)) + '</li>'
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
