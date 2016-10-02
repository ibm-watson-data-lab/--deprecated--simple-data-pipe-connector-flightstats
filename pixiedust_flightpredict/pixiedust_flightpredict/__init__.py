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

from pixiedust.display.display import *
from pixiedust.display import *
from .flightPredict import *
from .vizFeatures import *
import pixiedust
import pixiedust.utils.dataFrameMisc as dataFrameMisc

myLogger = pixiedust.getLogger(__name__)

@PixiedustDisplay()
class PixieDustFlightPredictPluginMeta(DisplayHandlerMeta):
  def createCategories(self):
    return [{"id":"FlightPredict","title":"Flight Predictor", "icon-path":"flightPredict.jpeg"}]
  @addId
  def getMenuInfo(self,entity):
    if entity==self.__class__:
      return [{"id": "flightpredict"}]
    elif dataFrameMisc.isPySparkDataFrame(entity):
      return [
        {"categoryId": "FlightPredict", "title": "Visualize Features", "icon":"fa-map-marker", "id":"fp_viz_features"},
        {"categoryId": "FlightPredict", "title": "Create Model", "icon":"fa-map-marker", "id":"fp_create_model"}
      ]
    return []

  def newDisplayHandler(self,options,entity):
    handlerId=options.get("handlerId")
    myLogger.debug("Creating a new Display Handler with id {0}".format(handlerId))
    if handlerId == "fp_viz_features":
      return VizualizeFeatures(options,entity)
    elif handlerId == "fp_create_model":
      return CreateModel(options,entity)
    else:
      return PixieDustFlightPredict(options,entity)

def flightPredict():
  display(PixieDustFlightPredictPluginMeta)

credentials={}
def setCredentials(**kwargs):
  credentials.update(kwargs)

def loadDataSet(dbName,sqlTable):
  if "cloudantHost" not in credentials or "cloudantUserName" not in credentials or "cloudantPassword" not in credentials:
    raise Exception("Missing credentials")
  cloudantdata = get_ipython().user_ns.get("sqlContext").read.format("com.cloudant.spark")\
    .option("cloudant.host",credentials.get("cloudantHost"))\
    .option("cloudant.username",credentials.get("cloudantUserName"))\
    .option("cloudant.password",credentials.get("cloudantPassword"))\
    .option("schemaSampleSize", "-1")\
    .load(dbName)

  cloudantdata.cache()
  print("Successfully cached dataframe")
  cloudantdata.registerTempTable(sqlTable)
  print("Successfully registered SQL table " + sqlTable);
  return cloudantdata