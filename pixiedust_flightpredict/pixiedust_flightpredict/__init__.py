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
import pixiedust
import pixiedust.utils.dataFrameMisc as dataFrameMisc
from pyspark.rdd import RDD
from pyspark.mllib.regression import LabeledPoint

myLogger = pixiedust.getLogger(__name__)

@PixiedustDisplay()
class PixieDustFlightPredictPluginMeta(DisplayHandlerMeta):
  def createCategories(self):
    return [{"id":"FlightPredict","title":"Flight Predictor", "icon-path":"flightPredict.jpeg"}]
  @addId
  def getMenuInfo(self,entity):
    if entity==self.__class__:
      return [{"id": "flightpredict"}]
    elif entity == "fp_configure_training":
      return [{"id": "fp_configure_training"}]
    elif dataFrameMisc.isPySparkDataFrame(entity):
      return [
        {"categoryId": "FlightPredict", "title": "Visualize Features", "icon-path":"vizFeatures.png", "id":"fp_viz_features"},
        {"categoryId": "FlightPredict", "title": "Configure Training", "icon":"fa-map-marker", "id":"fp_configure_training"}
      ]
    elif self.isLabeledRDD(entity):
      return [
        {"categoryId": "FlightPredict", "title": "Create Models", "icon-path":"vizFeatures.png", "id":"fp_create_models"},
        {"categoryId": "FlightPredict", "title": "Show Histogram", "icon-path":"vizFeatures.png", "id":"fp_histogram"}
      ]

    return []

  def isLabeledRDD(self, entity):
    if isinstance(entity,RDD):
      sample = entity.take(1)
      if sample is not None and len(sample)>0:
        return isinstance(sample[0], LabeledPoint)
    return False

  def newDisplayHandler(self,options,entity):
    handlerId=options.get("handlerId")
    myLogger.debug("Creating a new Display Handler with id {0}".format(handlerId))
    if handlerId == "fp_viz_features":
      import vizFeatures
      return vizFeatures.VizualizeFeatures(options,entity)
    elif handlerId == "fp_configure_training":
      import configureTraining
      return configureTraining.ConfigureTraining(options,entity)
    elif handlerId == "fp_create_models":
      import createModels
      return createModels.CreateModels(options, entity)
    elif handlerId == "fp_histogram":
      import histogramDisplay
      return histogramDisplay.HistogramDisplay(options, entity)
    else:
      return PixieDustFlightPredict(options,entity)

def flightPredict():
  display(PixieDustFlightPredictPluginMeta)

def configure():
  display("fp_configure_training")

class Configuration(object):
  __metaclass__= type("",(type,),{
        "configDict":{},
        "__getitem__":lambda cls, key: cls.configDict.get(key),
        "__setitem__":lambda cls, key,val: cls.configDict.update({key:val}),
        "__getattr__":lambda cls, key: cls.configDict.get(key),
        "__setattr__":lambda cls, key, val: cls.configDict.update({key:val})
    })
    
  @staticmethod
  def update(**kwargs):
    for key,val in kwargs.iteritems():
      Configuration[key]=val

def loadDataSet(dbName,sqlTable):
  if Configuration.cloudantHost is None or Configuration.cloudantUserName is None or Configuration.cloudantPassword is None:
    raise Exception("Missing credentials")
  cloudantdata = get_ipython().user_ns.get("sqlContext").read.format("com.cloudant.spark")\
    .option("cloudant.host",Configuration.cloudantHost)\
    .option("cloudant.username",Configuration.cloudantUserName)\
    .option("cloudant.password",Configuration.cloudantPassword)\
    .option("schemaSampleSize", "-1")\
    .load(dbName)

  cloudantdata.cache()
  print("Successfully cached dataframe")
  cloudantdata.registerTempTable(sqlTable)
  print("Successfully registered SQL table " + sqlTable);
  return cloudantdata