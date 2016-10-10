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
from requests.auth import HTTPBasicAuth
from pixiedust_flightpredict import Configuration
import pixiedust

myLogger = pixiedust.getLogger(__name__)

historyDBName = "/flightpredictorhistory"

def saveFlightResults(payload):
    auth=HTTPBasicAuth(Configuration.cloudantUserName, Configuration.cloudantPassword)

    #make sure the database is created first
    r = requests.put(Configuration.cloudantHost+historyDBName, auth=auth)
    if r.status_code != 200 and r.status_code != 201 and r.status_code != 412:
        return myLogger.error("Error connecting to the history database: {0}".format(r.text))

    depAirportInfo = payload["departureAirportInfo"]["airportInfo"]
    arrAirportInfo = payload["arrivalAirportInfo"]["airportInfo"]
    r = requests.post(Configuration.cloudantHost+historyDBName, auth=auth,json={
        'depAirportFSCode': depAirportInfo["fs"],
        'depAirportName': depAirportInfo["name"],
        'depAirportLong': depAirportInfo["longitude"],
        'depAirportLat': depAirportInfo["latitude"],
        'arrAirportFSCode': arrAirportInfo["fs"],
        'arrAirportName': arrAirportInfo["name"],
        'arrAirportLong': arrAirportInfo["longitude"],
        'arrAirportLat': arrAirportInfo["latitude"],
        'prediction': payload["prediction"]
    })
    if r.status_code != 200 and r.status_code != 201:
        return myLogger.error("Error saving flight results in database to the history database: {0}".format(r.text))

