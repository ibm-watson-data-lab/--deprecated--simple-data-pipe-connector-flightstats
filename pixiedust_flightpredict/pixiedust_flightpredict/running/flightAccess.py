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

def getFlightSchedule(flight, date):
    #parse the flight and date
    index = flight.rfind(" ")
    carrier = flight[:index]
    flightnumber = flight[index+1:]

    parts=date.split("-")
    if len(parts)!=3:
        raise ValueError("Invalid date {0}".format(date))

    year=parts[0]
    month=parts[1]
    day=parts[2]

    schedulesPath = "schedules/rest/v1/json/flight/{carrier}/{flightnumber}/departing/{year}/{month}/{day}"
    url = buildUrl(schedulesPath, carrier=carrier, flightnumber=flightnumber, year=year, month=month, day=day)
    myLogger.debug("Calling flight stats with url: " + url)
    response = requests.get( url )
    if response.status_code != 200:
        msg = "Error while trying to get schedule for flight {0}. Error is {1}".format(flight, str(response.reason))
        myLogger.error(msg)
        raise requests.HTTPError(msg, response=response)
        
    return response.json()


