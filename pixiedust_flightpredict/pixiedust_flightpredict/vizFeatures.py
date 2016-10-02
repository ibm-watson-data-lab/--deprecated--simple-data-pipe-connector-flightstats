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

class VizualizeFeatures(Mpld3ChartDisplay):

    def doRenderMpld3(self, handlerId, fig, ax, colormap, keyFields, keyFieldValues, keyFieldLabels, valueFields, valueFieldValues):
		paths = ax.scatter(valueFieldValues[0],valueFieldValues[1],c=valueFieldValues[1],marker='o',alpha=0.7,s=124,cmap=colormap)
		labels = []
		for i in range(len(valueFieldValues[0])):
			labels.append('({0},{1})'.format(valueFieldValues[0][i],valueFieldValues[1][i]))
		tooltip = mpld3.plugins.PointLabelTooltip(paths, labels=labels)
		mpld3.plugins.connect(fig, tooltip)
		ax.set_xlabel(valueFields[0], size=14)
		ax.set_ylabel(valueFields[1], size=14)