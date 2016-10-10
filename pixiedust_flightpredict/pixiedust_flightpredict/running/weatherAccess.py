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
import pixiedust_flightpredict
import pixiedust

myLogger = pixiedust.getLogger(__name__)

def getWeather(airportLat, airportLong, dtString):
    weatherUrl = pixiedust_flightpredict.Configuration.weatherUrl
    myLogger.debug("Using weatherUrl {0}".format(weatherUrl))
    #weatherUrl="https://4b88408f-11e5-4ddc-91a6-fbd442e84879:p6hxeJsfIb@twcservice.mybluemix.net"
    dt=parser.parse(dtString)
    url=weatherUrl +'/api/weather/v2/forecast/hourly/24hour'
    forecasts=requests.get(url, params=[('geocode',str(airportLat)+','+str(airportLong)),('units','m'),('language','en-US')]).json()['forecasts']
    #find the forecasts that is closest to dt.tm_hour
    weatherForecast=None
    for f in forecasts:
        ftime=datetime.fromtimestamp(f['fcst_valid'])
        if ftime.hour==dt.hour:
            weatherForecast=f
    return weatherForecast