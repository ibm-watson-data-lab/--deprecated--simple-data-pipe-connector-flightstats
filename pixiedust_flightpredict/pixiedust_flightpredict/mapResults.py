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
from pyspark.sql import functions as F
import yaml
from pixiedust_flightpredict.running.flightHistory import loadFlightHistory
import pixiedust
from pixiedust.utils.shellAccess import ShellAccess

myLogger = pixiedust.getLogger(__name__)

class MapResultsDisplay(Display):

    def doRender(self, handlerId):
        self.addProfilingTime = False
        self._addScriptElement("https://d3js.org/d3.v3.js", checkJSVar="d3")
        #self._addScriptElement("https://mbostock.github.io/d3/talk/20111116/d3/d3.geo.js")
        #self._addScriptElement("https://cdnjs.cloudflare.com/ajax/libs/d3-geo-projection/0.2.16/d3.geo.projection.js")
        #self._addScriptElement("https://mbostock.github.io/d3/talk/20111116/d3/d3.geom.js")

        #Load the data from the flight history db
        df = loadFlightHistory()
        ShellAccess.flightHistoryDF = df

        res = df.flatMap(lambda row: [\
                (row.depAirportFSCode, row.depAirportName.encode("ascii","ignore"), 0.0 if row.depAirportLat is None else row.depAirportLat,0.0 if row.depAirportLong is None else row.depAirportLong), \
                (row.arrAirportFSCode, row.arrAirportName.encode("ascii","ignore"), 0.0 if row.arrAirportLat is None else row.arrAirportLat,0.0 if row.arrAirportLong is None else row.arrAirportLong)\
            ])\
            .distinct()\
            .map(lambda t: """ "{0}":{{"id":"{0}","name":"{1}","latitude":{2},"longitude":{3}}}""".format(t[0], t[1], t[2],t[3]))

        graphNodesJson="{"
        for r in res.collect():
            graphNodesJson+=("," if len(graphNodesJson)>1 else "") + str(r)
        graphNodesJson+="}"
        myLogger.debug("graphNodesJson: {0}".format(graphNodesJson))        

        graphLinksJson = df.select("arrAirportFSCode","depAirportFSCode")\
            .withColumnRenamed("depAirportFSCode", "src").withColumnRenamed("arrAirportFSCode", "dst")\
            .groupBy("src","dst").agg(F.count("src").alias("count"))\
            .toJSON().map(lambda j: yaml.safe_load(j)).collect()
        myLogger.debug("graphLinksJson: {0}".format(graphLinksJson))


        self._addHTMLTemplate("mapResults.html", graphNodesJson=graphNodesJson, graphLinksJson=graphLinksJson)