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
from pixiedust.utils.shellAccess import ShellAccess
import pixiedust
import dateutil.parser as parser
from pixiedust.utils.template import *

myLogger = pixiedust.getLogger(__name__)

historyDBName = "flightpredictorhistory"

env = PixiedustTemplateEnvironment()

def saveFlightResults(payload):
    auth=HTTPBasicAuth(Configuration.cloudantUserName, Configuration.cloudantPassword)

    #make sure the database is created first
    url = Configuration.cloudantHost
    if "://" not in url:
        url = "https://"+url
    r = requests.put(url+ "/" + historyDBName, auth=auth)
    if r.status_code != 200 and r.status_code != 201 and r.status_code != 412:
        return myLogger.error("Error connecting to the history database: {0}".format(r.text))

    depAirportInfo = payload["departureAirportInfo"]["airportInfo"]
    arrAirportInfo = payload["arrivalAirportInfo"]["airportInfo"]
    r = requests.post(url+"/" + historyDBName, auth=auth,json={
        'depAirportFSCode': depAirportInfo["fs"],
        'depAirportName': depAirportInfo["name"],
        'depAirportLong': depAirportInfo["longitude"],
        'depAirportLat': depAirportInfo["latitude"],
        'arrAirportFSCode': arrAirportInfo["fs"],
        'arrAirportName': arrAirportInfo["name"],
        'arrAirportLong': arrAirportInfo["longitude"],
        'arrAirportLat': arrAirportInfo["latitude"],
        'prediction': payload["prediction"],
        'carrierFsCode': payload["flightInfo"]["carrierFsCode"],
        'flightNumber': payload["flightInfo"]["flightNumber"],
        'departureDate': parser.parse( payload["flightInfo"]["departureTime"]).strftime("%B %d %Y"),
        'departureTime': parser.parse( payload["flightInfo"]["departureTime"]).strftime("%I:%M %p")
    })
    if r.status_code != 200 and r.status_code != 201:
        return myLogger.error("Error saving flight results in database to the history database: {0}".format(r.text))
"""
Load the flight history into a dataFrame
"""
def loadFlightHistory():
    if Configuration.cloudantHost is None or Configuration.cloudantUserName is None or Configuration.cloudantPassword is None:
        raise Exception("Missing credentials")
    return ShellAccess.sqlContext.read.format("com.cloudant.spark")\
        .option("cloudant.host",Configuration.cloudantHost)\
        .option("cloudant.username",Configuration.cloudantUserName)\
        .option("cloudant.password",Configuration.cloudantPassword)\
        .option("schemaSampleSize", "-1")\
        .load(historyDBName)

def getBadgeHtml(depAirportCode, arrAirportCode):
    if ShellAccess.flightHistoryDF is None:
        myLogger.info("Reloading flight history from getBadgeHtml")
        ShellAccess.flightHistoryDF = loadFlightHistory()
    
    df = ShellAccess.flightHistoryDF
    res = df.filter( df['depAirportFSCode']== depAirportCode )\
        .filter( df['arrAirportFSCode']==arrAirportCode )\
        .map(lambda t: ((t["carrierFsCode"], t["flightNumber"], t["departureDate"],t["departureTime"], t["depAirportName"], t["arrAirportName"]), 1))\
        .reduceByKey(lambda t,v : t+v)
    
    return env.getTemplate("flightBadge.html").render(flights=res.collect())


