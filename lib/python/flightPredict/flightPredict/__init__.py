from pyspark.sql import SQLContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.evaluation import MulticlassMetrics
from IPython.display import display, HTML
import matplotlib.pyplot as plt

cloudantHost=None
cloudantUserName=None
cloudantPassword=None
sqlContext=None
weatherUrl=None
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
    .load(dbName)
    
    cloudantdata.registerTempTable(sqlTable)
    print("Successfully registered SQL table " + sqlTable);
    return cloudantdata

attributes=['dewPt','rh','vis','wc','wdir','wspd','feels_like','uv_index']
def buildLabeledPoint(s):
    features=[]
    for attr in attributes:
        features.append(getattr(s, attr + '_1'))
    for attr in attributes:
        features.append(getattr(s, attr + '_2'))
    return LabeledPoint(s.classification,Vectors.dense(features))

def loadLabeledDataRDD(sqlTable):
    select = 'select '
    comma=''
    for attr in attributes:
        select += comma + 'departureWeather.' + attr + ' as ' + attr + '_1'
        comma=','

    select += ',classification'
    for attr in attributes:
        select += comma + 'arrivalWeather.' + attr + ' as ' + attr + '_2'
		
    select += ' from ' + sqlTable
	
    df = sqlContext.sql(select)

    datardd = df.map(lambda s: buildLabeledPoint(s))
    datardd.cache()
    return datardd
    
def runMetrics(labeledDataRDD, *args):
    html='<table width=100%><tr><th>Model</th><th>Accuracy</th><th>Precision</th><th>Recall</th></tr>'
    for model in args:
        label= model.__class__.__name__
        predictionAndLabels = model.predict(labeledDataRDD.map(lambda lp: lp.features))
        metrics = MulticlassMetrics(\
            predictionAndLabels.zip(labeledDataRDD.map(lambda lp: lp.label)).map(lambda t: (float(t[0]),float(t[1])))\
        )
        html+='<tr><td>{0}</td><td>{1:.2f}%</td><td>{2:.2f}%</td><td>{3:.2f}%</td></tr>'\
            .format(label,metrics.weightedFMeasure(beta=1.0)*100, metrics.weightedPrecision*100,metrics.weightedRecall*100 )
    html+='</table>'
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