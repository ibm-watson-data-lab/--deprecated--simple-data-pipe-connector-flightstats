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
from pixiedust.utils.shellAccess import ShellAccess
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import Vectors
import pixiedust_flightpredict
import pixiedust

myLogger = pixiedust.getLogger(__name__)

__all__ = ['defaultTrainingHandler', 'loadLabeledDataRDD', 'getNumClasses', 'getTrainingHandler', 'attributes']

attributes=['dewPt','rh','vis','wc',
    #'wdir',
    'wspd','feels_like','uv_index']
attributesMsg = ['Dew Point', 'Relative Humidity', 'Prevailing Hourly visibility', 'Wind Chill', 
     #'Wind direction',
    'Wind Speed','Feels Like Temperature', 'Hourly Maximum UV Index']

#Display Confusion Matrix as an HTML table when computing metrics
displayConfusionTable=False

def buildLabeledPoint(s, classification, handler):
    features=[]
    for attr in attributes:
        features.append(getattr(s, attr + '_1'))
    for attr in attributes:
        features.append(getattr(s, attr + '_2'))
    customFeatures=handler.customTrainingFeatures(s)
    for v in customFeatures:
        features.append(v)
    return LabeledPoint(classification,Vectors.dense(features))

#default training handler class
class defaultTrainingHandler:
    defTrainingHandler = None
    def getClassLabel(self, value):
        if ( int(value)==0 ):
            return "Canceled"
        elif (int(value)==1 ):
            return "On Time"
        elif (int(value) == 2 ):
            return "Delayed less than 2 hours"
        elif (int(value) == 3 ):
            return "Delayed between 2 and 4 hours"
        elif (int(value) == 4 ):
            return "Delayed more than 4 hours"
        return value
        
    def numClasses(self):
        return 5
    
    def computeClassification(self, s):
        return s.classification
    
    def customTrainingFeaturesNames(self ):
        return []
    
    def customTrainingFeatures(self, s):
        return []
    
def getTrainingHandler():
    customHandlers = [ShellAccess[x] for x in ShellAccess if isinstance(ShellAccess[x], defaultTrainingHandler)]
    customTrainingHandler =  None if len(customHandlers)==0 else customHandlers[0]
    if customTrainingHandler is None:
        myLogger.debug("Using default customTrainingHandler")
        if defaultTrainingHandler.defTrainingHandler is None:
            defaultTrainingHandler.defTrainingHandler = defaultTrainingHandler()
        customTrainingHandler=defaultTrainingHandler.defTrainingHandler
    return customTrainingHandler

def getNumClasses():
    return getTrainingHandler().numClasses()
    
def loadLabeledDataRDD(sqlTable):    
    select = 'select '
    comma=''
    for attr in attributes:
        select += comma + 'departureWeather.' + attr + ' as ' + attr + '_1'
        comma=','
    select += ',deltaDeparture'
    select += ',classification'
    for attr in attributes:
        select += comma + 'arrivalWeather.' + attr + ' as ' + attr + '_2'
    
    for attr in getTrainingHandler().customTrainingFeaturesNames():
        select += comma + attr
    select += ' from ' + sqlTable
    
    df = ShellAccess.sqlContext.sql(select)

    handler=getTrainingHandler()
    datardd = df.map(lambda s: buildLabeledPoint(s, handler.computeClassification(s), handler))
    datardd.cache()
    return datardd

