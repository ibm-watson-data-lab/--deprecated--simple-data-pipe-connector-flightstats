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
from pixiedust.display.chart.mpld3ChartDisplay import Mpld3ChartDisplay
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
from pixiedust.utils.shellAccess import ShellAccess

class HistogramDisplay(Mpld3ChartDisplay):
    def doRender(self, handlerId):
        rdd = ShellAccess.sqlContext.sql("select deltaDeparture from training").map(lambda s: s.deltaDeparture)\
                .filter(lambda s: s < 50 and s > 12)

        histo = rdd.histogram(50)
        bins = [i for i in histo[0]]

        fig, ax = plt.subplots(figsize=(12,8))

        ax.set_ylabel('Number of records')
        ax.set_xlabel('Bin')
        plt.title('Histogram')
        intervals = [abs(j-i) for i,j in zip(bins[:-1], bins[1:])]
        values=[sum(intervals[:i]) for i in range(0,len(intervals))]
        ax.bar(values, histo[1], intervals, color='b', label = "Bins")
        ax.set_xticks(bins[:-1],[int(i) for i in bins[:-1]])
        ax.legend()

        #Render the figure
        (dialogTemplate, dialogOptions) = self.getDialogInfo(handlerId)
        dialogBody=self.renderTemplate(dialogTemplate, **dialogOptions)
        self.renderFigure(fig, dialogBody)

    def doRenderMpld3(self, handlerId, fig, ax, keyFields, keyFieldValues, keyFieldLabels, valueFields, valueFieldValues):
        pass
