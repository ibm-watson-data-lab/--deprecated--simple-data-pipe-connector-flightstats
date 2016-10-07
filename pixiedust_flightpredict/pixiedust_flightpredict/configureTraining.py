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
import pixiedust_flightpredict

ICON_FAILED = "fa-times"
ICON_SUCCESS = "fa-check"
class ConfigureTraining(Display):
    def doRender(self, handlerId):
        self.addProfilingTime = False
        #update the configuration
        pixiedust_flightpredict.Configuration.update( **self.options )

        steps=[
            {"title": "Architecture", "template": "step_welcome.html"},
            {"title": "Credentials", "template": "step_credentials.html","args":[
                ("cloudantHost","Cloudant Host"),("cloudantUserName","Cloudant User Name"),("cloudantPassword","Cloudant Password"),
                ("weatherUrl", "Weather URL")
            ]},
            {"title": "Training Sets", "template": "step_sets.html", "args":[
                ('Training Set', 'training', [('Database Name', 'DbName',''),('SQL Table Name', 'SQLTableName','training'),('DataFrame Variable Name', 'DFTrainingVarName','trainingData')]), 
                ('Test Set', 'test', [('Database Name', 'DbName',''),('SQL Table Name', 'SQLTableName','test'), ('DataFrame Variable Name', 'DFTestVarName','testData')])
            ]}
        ]

        if self.options.get("nostore_edit")=='true':
            self._addHTMLTemplate("configureWizard.html", steps=steps);
        else:
            tasks=[
                self.checkConfigParams(["cloudantHost","cloudantUserName","cloudantPassword"], "Cloudant Configuration is OK"),
                self.checkConfigParams(["weatherUrl"], "WeatherUrl Configuration is OK"),
                self.checkDataSet( steps[2]["args"][0]),
                self.checkDataSet( steps[2]["args"][1]),
            ]
            self._addHTMLTemplate("configureWizard.html", tasks = tasks )

    def checkConfigParams(self, fields, okDescription=''):
        for f in fields:
            if pixiedust_flightpredict.Configuration[f] is None:
                return { "status-class": ICON_FAILED,
                         "task": "Missing configuration {0}".format(f),
                         "action": "Click on the edit configuration button and provide the require parameter"
                        }
        return { "status-class": ICON_SUCCESS,
                 "task": okDescription,
                 "action": "None"
                }

    def checkDataSet(self, datasetInfo):
        task = self.checkConfigParams([datasetInfo[1]+"DbName", datasetInfo[1]+"SQLTableName"])
        if task["status-class"] == ICON_FAILED:
            return task

        #First check the DataFrame Variable Name exists
        def findVarName():
            for v in datasetInfo[2]:
                if v[0]=="DataFrame Variable Name":
                    return v[2]
        
        varName = findVarName()
        if varName not in get_ipython().user_ns:
            code = """ \\"dbName='{dbName}'\\\\n{varName} = pixiedust_flightpredict.loadDataSet(dbName,'{sqlTableName}')\\\\ndisplay({varName})\\" """\
                .format(varName=varName,dbName=pixiedust_flightpredict.Configuration[datasetInfo[1]+"DbName"], sqlTableName=pixiedust_flightpredict.Configuration[datasetInfo[1]+"SQLTableName"])
            return { "status-class": ICON_FAILED,   
                    "task": "The variable {0} is not defined. If you already have a cell that loads it, please run it now. If not, click on the action button to generate a new cell".format(varName),
                    "action": "Generate Cell code to load {0}".format(varName),
                    "code": "get_ipython().set_next_input({0})".format(code),
                    "id": varName
                    }

        return { "status-class": ICON_SUCCESS,
                 "task": "dataframe {0} correctly loaded".format(varName),
                 "action": "None"
                }
