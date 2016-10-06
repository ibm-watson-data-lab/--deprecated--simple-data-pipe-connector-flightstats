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

import training
from pixiedust.display.chart.mpld3ChartDisplay import Mpld3ChartDisplay
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.cm as cm
from pyspark.sql import Row

def makeList(l):
    return l if isinstance(l, list) else [l]

class VizualizeFeatures(Mpld3ChartDisplay):

    def doRender(self, handlerId):
        f1="departureWeather.temp"
        f2="arrivalWeather.temp"
        f1=f1.split(".")
        f2=f2.split(".")
        handler=training.getTrainingHandler()
        darr=self.entity.map(lambda s: ( handler.computeClassification(s),(\
            reduce(lambda x,y: getattr(x,y) if isinstance(x, Row) else getattr(getattr(s,x),y), f1) if len(f1)>1 else getattr(s,f1[0]),\
            reduce(lambda x,y: getattr(x,y) if isinstance(x, Row) else getattr(getattr(s,x),y), f2) if len(f2)>1 else getattr(s,f2[0])\
            )))\
            .reduceByKey(lambda x,y: makeList(x) + makeList(y))\
            .collect()
        numClasses=handler.numClasses()
        citer=iter(cm.rainbow(np.linspace(0, 1, numClasses)))
        colors = [next(citer) for i in range(0, numClasses)]
        legends= [handler.getClassLabel(i) for i in range(0,numClasses)]
        sets=[]
        fig, ax = plt.subplots(figsize=(12,8))
        for t in darr:
            sets.append((ax.scatter([x[0] for x in t[1]],[x[1] for x in t[1]],color=colors[t[0]],alpha=0.5),legends[t[0]]))

        ax.set_ylabel("Departure Airport Temp")
        ax.set_xlabel("Arrival Airport Temp")
        ax.legend([x[0] for x in sets],
                [x[1] for x in sets],
                scatterpoints=1,
                loc='lower left',
                ncol=numClasses,
                fontsize=12)

        #Render the figure
        (dialogTemplate, dialogOptions) = self.getDialogInfo(handlerId)
        dialogBody=self.renderTemplate(dialogTemplate, **dialogOptions)
        self.renderFigure(fig, dialogBody)

    def doRenderMpld3(self, handlerId, fig, ax, keyFields, keyFieldValues, keyFieldLabels, valueFields, valueFieldValues):
        pass