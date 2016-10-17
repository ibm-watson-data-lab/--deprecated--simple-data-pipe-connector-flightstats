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
import os
import pixiedust

myLogger=pixiedust.getLogger(__name__)

flexBaseUrl = "https://api.flightstats.com/flex/"

#Flight stats appId and appKey
appId=os.environ["appId"]
appKey=os.environ["appKey"]

def buildUrl(path, **kwargs):
    if appId is None or appKey is None:
        raise ValueError("appId or appKey is not defined")
    return (flexBaseUrl+path).format(**kwargs) + "?appId={0}&appKey={1}".format(appId, appKey)

def parseDate(date):
    parts=date.split("-")
    if len(parts)!=3:
        raise ValueError("Invalid date {0}".format(date))

    #(year, month, day)
    return (parts[0], parts[1], parts[2])

def getFlightSchedule(flight, date):
    #parse the flight and date
    index = flight.rfind(" ")
    carrier = flight[:index]
    flightnumber = flight[index+1:]

    (year,month,day) = parseDate(date)

    schedulesPath = "schedules/rest/v1/json/flight/{carrier}/{flightnumber}/departing/{year}/{month}/{day}"
    url = buildUrl(schedulesPath, carrier=carrier, flightnumber=flightnumber, year=year, month=month, day=day)
    myLogger.debug("Calling flight stats with url: " + url)
    response = requests.get( url )
    if response.status_code != 200:
        msg = "Error while trying to get schedule for flight {0}. Error is {1}".format(flight, str(response.reason))
        myLogger.error(msg)
        raise requests.HTTPError(msg, response=response)
        
    return response.json()

flightsCache={}
def getFlights(airport, date, hour):
    (year,month,day) = parseDate(date)
    #check the cache first
    if len(hour)>2:
        hour = hour[:2]
    key = airport+date+str(hour)
    if key in flightsCache:
        return flightsCache[key]

    path = "schedules/rest/v1/json/from/{departureAirportCode}/departing/{year}/{month}/{day}/{hourOfDay}"

    url = buildUrl(path, departureAirportCode=airport, year=year,month=month,day=day,hourOfDay=hour)
    myLogger.debug("Calling getFlights with url: " + url)
    response = requests.get( url )
    if response.status_code != 200:
        msg = "Error while trying to get flights for airport {0} at hour {1}. Error is {2}".format(airport, hour, str(response.reason))
        myLogger.error(msg)
        raise requests.HTTPError(msg, response=response)

    payload = response.json()
    if "error" in payload:
        msg = "Error while trying to get flights for airport {0} at hour {1}. Error is {2}".format(airport, hour, payload['error']['errorMessage'])
        myLogger.error(msg)
        raise requests.HTTPError(msg, response = response)

    ret = {
        "scheduledFlights": payload['scheduledFlights'],
        "airports": payload['appendix']['airports'] if 'airports' in payload['appendix'] else [],
        "airlines": payload['appendix']['airlines'] if 'airlines' in payload['appendix'] else []
    }
    flightsCache[key]=ret
    return ret



