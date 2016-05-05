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

from pyspark.sql import SQLContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.evaluation import MulticlassMetrics
from IPython.display import display, HTML
import matplotlib.pyplot as plt

#global variables
#credentials
cloudantHost=None
cloudantUserName=None
cloudantPassword=None
sqlContext=None
weatherUrl=None

attributes=['dewPt','rh','vis','wc',
    #'wdir',
    'wspd','feels_like','uv_index']
attributesMsg = ['Dew Point', 'Relative Humidity', 'Prevailing Hourly visibility', 'Wind Chill', 
     #'Wind direction',
    'Wind Speed','Feels Like Temperature', 'Hourly Maximum UV Index']

#Function used to customize classification of data
customTrainingHandler=None

#Display Confusion Matrix as an HTML table when computing metrics
displayConfusionTable=False

def loadDataSet(dbName,sqlTable):
    if (sqlContext==None):
        raise Exception("sqlContext not set")
    if (cloudantHost==None):
        raise Exception("cloudantHost not set")
    if (cloudantUserName==None):
        raise Exception("cloudantUserName not set")
    if (cloudantPassword==None):
        raise Exception("cloudantPassword not set")
    cloudantdata = sqlContext.read.format("com.cloudant.spark")\
    .option("cloudant.host",cloudantHost)\
    .option("cloudant.username",cloudantUserName)\
    .option("cloudant.password",cloudantPassword)\
    .option("schemaSampleSize", "-1")\
    .load(dbName)
    
    cloudantdata.cache()
    print("Successfully cached dataframe")
    cloudantdata.registerTempTable(sqlTable)
    print("Successfully registered SQL table " + sqlTable);
    return cloudantdata

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
    global customTrainingHandler
    if ( customTrainingHandler == None ):
        customTrainingHandler=defaultTrainingHandler()
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
    
    df = sqlContext.sql(select)

    handler=getTrainingHandler()
    datardd = df.map(lambda s: buildLabeledPoint(s, handler.computeClassification(s), handler))
    datardd.cache()
    return datardd
    
def runMetrics(labeledDataRDD, *args):
    html='<table width=100%><tr><th>Model</th><th>Accuracy</th><th>Precision</th><th>Recall</th></tr>'
    confusionHtml = '<p>Confusion Tables for each Model</p>'
    for model in args:
        label= model.__class__.__name__
        predictionAndLabels = model.predict(labeledDataRDD.map(lambda lp: lp.features))
        metrics = MulticlassMetrics(\
            predictionAndLabels.zip(labeledDataRDD.map(lambda lp: lp.label)).map(lambda t: (float(t[0]),float(t[1])))\
        )
        html+='<tr><td>{0}</td><td>{1:.2f}%</td><td>{2:.2f}%</td><td>{3:.2f}%</td></tr>'\
            .format(label,metrics.weightedFMeasure(beta=1.0)*100, metrics.weightedPrecision*100,metrics.weightedRecall*100 )

        if ( displayConfusionTable ):
            #get labels from RDD
            handler=getTrainingHandler()
            classLabels = labeledDataRDD.map(lambda t: t.label).distinct().map(lambda l: handler.getClassLabel(l)).collect()
            confusionMatrix = metrics.call("confusionMatrix")
            confusionMatrixArray = confusionMatrix.toArray()
            #labels = metrics.call("labels")
            confusionHtml += "<p>" + label + "<p>"
            confusionHtml += "<table>"
            confusionHtml+="<tr><td></td>"
            for classLabel in classLabels:
                confusionHtml+="<td>" + str(classLabel) + "</td>"
            confusionHtml+="</tr>"
            
            for i, row in enumerate(confusionMatrixArray):
                confusionHtml += "<tr>"
                confusionHtml += "<td>" + classLabels[i] + "</td>"
                for j, cell in enumerate(row):
                    confusionHtml+="<td style='text-align:center'>" + ("<b>" if (i==j) else "") +  str(cell) + ("</b>" if (i==j) else "") + "</td>"
                confusionHtml += "</tr>"
            confusionHtml += "</table>"
        
    html+='</table>'
    
    if ( displayConfusionTable ):
        html+=confusionHtml
    
    display(HTML(html))
    
def makeList(l):
    return l if isinstance(l, list) else [l]
def scatterPlotForFeatures(df, f1,f2,legend1,legend2):
    darr = df.select(f1,"classification", f2)\
        .map(lambda r: (r[1],(r[0],r[2])))\
        .reduceByKey(lambda x,y: makeList(x) + makeList(y))\
        .collect()
    colors = ["yellow", "red", "black", "blue", "green"]
    legends= ["Canceled", "On Time", "Delay < 2h", "2h<delay<4h", "delay>4h"]
    sets=[]
    for t in darr:
        sets.append((plt.scatter([x[0] for x in t[1]],[x[1] for x in t[1]], color=colors[t[0]],alpha=0.5),legends[t[0]]))

    params = plt.gcf()
    plSize = params.get_size_inches()
    params.set_size_inches( (plSize[0]*3, plSize[1]*2) )
    plt.ylabel(legend2)
    plt.xlabel(legend1)
    plt.legend([x[0] for x in sets],
               [x[1] for x in sets],
               scatterpoints=1,
               loc='lower left',
               ncol=5,
               fontsize=12)
    plt.show()