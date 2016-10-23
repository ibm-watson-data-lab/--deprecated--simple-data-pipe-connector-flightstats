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
import pytz
from dateutil.parser import parse
import datetime
from pixiedust_flightpredict import Configuration

myLogger=pixiedust.getLogger(__name__)

flexBaseUrl = "https://api.flightstats.com/flex/"

def buildUrl(path, **kwargs):
    appId=os.environ.get("appId") or Configuration["appId"]
    appKey=os.environ.get("appKey") or Configuration["appKey"]

    if appId is None or appKey is None:
        raise ValueError("appId or appKey is not defined")
    return (flexBaseUrl+path).format(**kwargs) + "?appId={0}&appKey={1}".format(appId, appKey)

def toUTC(sDateTime, timeZoneRegionName):
    return pytz.timezone( timeZoneRegionName ).localize(parse(sDateTime)).astimezone (pytz.utc)

def parseDate(date):
    dt = pytz.utc.localize(datetime.datetime.utcfromtimestamp(date/1000))
    return (dt.year,dt.month,dt.day,dt.hour)

def getFlightSchedule(flight, date):
    myLogger.debug("getFlightSchedule with args: {0}, {1}".format( flight, date ) )
    #parse the flight and date
    index = flight.rfind(" ")
    carrier = flight[:index]
    flightnumber = flight[index+1:]

    (year,month,day,hour) = parseDate(date)

    schedulesPath = "schedules/rest/v1/json/flight/{carrier}/{flightnumber}/departing/{year}/{month}/{day}"
    url = buildUrl(schedulesPath, carrier=carrier, flightnumber=flightnumber, year=year, month=month, day=day)
    myLogger.debug("Calling flight stats with url: " + url)
    response = requests.get( url )
    if response.status_code != 200:
        msg = "Error while trying to get schedule for flight {0}. Error is {1}".format(flight, str(response.reason))
        myLogger.error(msg)
        raise requests.HTTPError(msg, response=response)
        
    payload = response.json()

    def findAirport(airportCode):
        for airport in payload['appendix']['airports']:
            if airport['fs'] == airportCode:
                return airport
        myLogger.error("error find airport {0} from getFlightSchedule".format(airportCode))

    def findEquipment(code):
        for equipment in payload['appendix']['equipments']:
            if equipment['iata'] == code:
                return equipment
        myLogger.error("error find equipment {0} from getFlightSchedule".format(code))
        return {}

    def findAirline(code):
        for airline in payload['appendix']['airlines']:
            if airline['fs'] == code:
                return airline
        myLogger.error("error find airline {0} from getFlightSchedule".format(code))
        return {}

    #find the right flight as there may be more than one
    scheduledFlights = payload.pop("scheduledFlights")
    thisFlight = None
    if len(scheduledFlights)>1:
        for scheduledFlight in scheduledFlights:
            airport = findAirport(scheduledFlight['departureAirportFsCode'])
            if airport is not None:
                utcDT = toUTC( scheduledFlight["departureTime"], airport['timeZoneRegionName'])
                myLogger.info("Comparing time for airport {0} between {1} and {2}".format( airport['name'], utcDT.hour, hour))
                if utcDT.hour == hour:
                    thisFlight = scheduledFlight
                    thisFlight['departureTimeUTC'] = str(utcDT)
                    arrAirport = findAirport( scheduledFlight['arrivalAirportFsCode'])
                    thisFlight['arrivalTimeUTC'] = str( toUTC(scheduledFlight['arrivalTime'], arrAirport['timeZoneRegionName']))
                    break
    else:
        thisFlight = scheduledFlights[0]

    if thisFlight:
        airport = findAirport(thisFlight['departureAirportFsCode'])
        thisFlight['departureTimeUTC'] = str(toUTC( thisFlight["departureTime"], airport['timeZoneRegionName']))
        arrAirport = findAirport( thisFlight['arrivalAirportFsCode'])
        thisFlight['arrivalTimeUTC'] = str( toUTC(thisFlight['arrivalTime'], arrAirport['timeZoneRegionName']))
        myLogger.info("Found flight corresponding to flight {0}: {1}".format(flight, thisFlight))
        payload['scheduledFlight']=thisFlight

        #find equipment and airline info for this flight
        thisFlight['equipmentInfo']=findEquipment(thisFlight['flightEquipmentIataCode'])
        thisFlight['airlineInfo']=findAirline(thisFlight['carrierFsCode'])
    else:
        raise Exception("Unable to find flight corresponding to flight {0}".format(flight))

    payload.pop('request')
    return payload

flightsCache={}
def getFlights(airport, date, hour):
    parts=date.split("-")
    if len(parts)!=3:
        raise ValueError("Invalid date {0}".format(date))

    (year,month,day) = (parts[0], parts[1], parts[2])
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

    #convert departuretimes from local to UTC
    def findAirport(airportCode):
        for airport in payload['appendix']['airports']:
            if airport['fs'] == airportCode:
                return airport
    for flight in payload['scheduledFlights']:
        airport = findAirport( flight["departureAirportFsCode"] )
        if airport is not None:
            dt = parse(flight["departureTime"])
            flight["departureTimeUTC"] = str(pytz.timezone(airport['timeZoneRegionName']).localize(dt).astimezone (pytz.utc))
        else:
            myLogger.error("Unable to resolve airport code {0} because it is not in the appendix returned by flightstats".format(flight["departureAirportFsCode"]))

    ret = {
        "scheduledFlights": payload['scheduledFlights'],
        "airports": payload['appendix']['airports'] if 'airports' in payload['appendix'] else [],
        "airlines": payload['appendix']['airlines'] if 'airlines' in payload['appendix'] else []
    }
    flightsCache[key]=ret
    return ret



