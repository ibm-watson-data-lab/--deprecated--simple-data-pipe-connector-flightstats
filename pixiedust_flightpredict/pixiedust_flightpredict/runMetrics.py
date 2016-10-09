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
from pixiedust_flightpredict import Configuration
import pixiedust_flightpredict.training as training
from pyspark.mllib.evaluation import MulticlassMetrics

class RunMetricsDisplay(Display):
    def doRender(self, handlerId):
        html='<table width=100%><tr><th>Model</th><th>Accuracy</th><th>Precision</th><th>Recall</th></tr>'
        confusionHtml = '<p>Confusion Tables for each Model</p>'
        for modelName,model in Configuration.getModels():
            label= model.__class__.__name__
            labeledDataRDD, sqlTableName = Configuration.getLabeledData(self.entity)
            predictionAndLabels = model.predict(labeledDataRDD.map(lambda lp: lp.features))
            metrics = MulticlassMetrics(\
                predictionAndLabels.zip(labeledDataRDD.map(lambda lp: lp.label)).map(lambda t: (float(t[0]),float(t[1])))\
            )
            html+='<tr><td>{0}</td><td>{1:.2f}%</td><td>{2:.2f}%</td><td>{3:.2f}%</td></tr>'\
                .format(label,metrics.weightedFMeasure(beta=1.0)*100, metrics.weightedPrecision*100,metrics.weightedRecall*100 )
            displayConfusionTable = True
            if ( displayConfusionTable ):
                #get labels from RDD
                handler=training.getTrainingHandler()
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
        
        self._addHTML(html)