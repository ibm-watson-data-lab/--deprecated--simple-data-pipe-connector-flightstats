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

def loadLabeledDataRDD(sqlTable):
    df = sqlContext.sql("select \
    	departureWeather.pressure_tend as pressure_tend_1 \
    	,departureWeather.dewPt as dewPt_1 \
    	,departureWeather.heat_index as heat_index_1 \
    	,departureWeather.rh as rh_1 \
    	,departureWeather.pressure as pressure_1 \
    	,departureWeather.vis as vis_1 \
    	,departureWeather.wc as wc_1 \
    	,departureWeather.wdir as wdir_1 \
	    ,departureWeather.wspd as wspd_1 \
	    ,departureWeather.max_temp as max_temp_1 \
	    ,departureWeather.min_temp as min_temp_1 \
	    ,departureWeather.precip_total as precip_total_1 \
	    ,departureWeather.precip_hrly as precip_hrly_1 \
	    ,departureWeather.snow_hrly as snow_hrly_1 \
	    ,departureWeather.feels_like as feels_like_1 \
	    ,departureWeather.uv_index as uv_index_1 \
	    ,classification\
	    ,arrivalWeather.pressure_tend as pressure_tend_2 \
	    ,arrivalWeather.dewPt as dewPt_2 \
	    ,arrivalWeather.heat_index as heat_index_2 \
	    ,arrivalWeather.rh as rh_2 \
	    ,arrivalWeather.pressure as pressure_2 \
	    ,arrivalWeather.vis as vis_2 \
	    ,arrivalWeather.wc as wc_2 \
	    ,arrivalWeather.wdir as wdir_2 \
	    ,arrivalWeather.wspd as wspd_2 \
	    ,arrivalWeather.max_temp as max_temp_2 \
	    ,arrivalWeather.min_temp as min_temp_2 \
	    ,arrivalWeather.precip_total as precip_total_2 \
	    ,arrivalWeather.precip_hrly as precip_hrly_2 \
	    ,arrivalWeather.snow_hrly as snow_hrly_2 \
	    ,arrivalWeather.feels_like as feels_like_2 \
	    ,arrivalWeather.uv_index as uv_index_2 \
	    from " + sqlTable \
    )

    datardd = df.map(lambda s: LabeledPoint(\
        s.classification, \
        Vectors.dense([\
           s.pressure_tend_1,\
           s.dewPt_1,\
           s.heat_index_1,\
           s.rh_1,\
           s.pressure_1,\
           s.vis_1,\
           s.wc_1,\
           s.wdir_1,\
           s.wspd_1,\
           s.max_temp_1,\
           s.min_temp_1,\
           s.precip_total_1,\
           s.precip_hrly_1,\
           s.snow_hrly_1,\
           s.feels_like_1,\
           s.uv_index_1,\
           s.pressure_tend_2,\
           s.dewPt_2,\
           s.heat_index_2,\
           s.rh_2,\
           s.pressure_2,\
           s.vis_2,\
           s.wc_2,\
           s.wdir_2,\
           s.wspd_2,\
           s.max_temp_2,\
           s.min_temp_2,\
           s.precip_total_2,\
           s.precip_hrly_2,\
           s.snow_hrly_2,\
           s.feels_like_2,\
           s.uv_index_2\
        ])\
     )\
    )
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