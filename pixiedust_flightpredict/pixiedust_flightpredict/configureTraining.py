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
from pixiedust.utils.shellAccess import ShellAccess
from pixiedust_flightpredict.training.training import *
from pixiedust.utils.template import PixiedustTemplateEnvironment

ICON_FAILED = "fa-times"
ICON_SUCCESS = "fa-check"
ICON_INFO = "fa-info"

DFTrainingVarName = 'trainingData'
LabeledRDDTrainingVarName = 'labeledTrainingData'
LabeledRDDTestVarName = 'labeledTestData'


class ConfigureTraining(Display):
    def loadTemplate(self, templateName, **kwargs):
         return '\\"' + self.renderTemplate(templateName,**kwargs).replace('\n','\\\\n').replace('"', '\\\\\\"') + '\\"'

    def doRender(self, handlerId):
        self.addProfilingTime = False
        #update the configuration
        pixiedust_flightpredict.Configuration.update( **self.options )

        steps=[
            {"title": "Architecture", "template": "step_welcome.html"},
            {"title": "Credentials", "template": "step_credentials.html","args":[
                ("cloudantHost","Cloudant Host"),("cloudantUserName","Cloudant User Name"),("cloudantPassword","Cloudant Password"),
                ("weatherUrl", "Weather URL"),("appId", "FlightStats API Id"),("appKey", "FlightStats API Key")
            ]},
            {"title": "Training Set", "template": "step_sets.html", "args":[
                ('Database Name', 'TrainingDbName',''),
                ('SQL Table Name', 'TrainingSQLTableName','training'),
                ('DataFrame Variable Name', 'DFTrainingVarName', DFTrainingVarName), 
                ('LabeledRDD Variable Name', 'LabeledRDDTrainingVarName', LabeledRDDTrainingVarName)
            ]},
            {"title": "Test Set", "template": "step_sets.html", "args":[
                ('Database Name', 'TestDbName',''),
                ('SQL Table Name', 'TestSQLTableName','test'), 
                ('DataFrame Variable Name', 'DFTestVarName','testData'), 
                ('LabeledRDD Variable Name', 'LabeledRDDTestVarName', LabeledRDDTestVarName)
            ]}
        ]

        if self.options.get("nostore_edit")=='true':
            self._addHTMLTemplate("configureWizard.html", steps=steps);
        else:
            tasks=[
                self.checkConfigParams(["cloudantHost","cloudantUserName","cloudantPassword"], "Cloudant Configuration is OK"),
                self.checkConfigParams(["weatherUrl"], "WeatherUrl Configuration is OK"),
                self.checkConfigParams(["appId", "appKey"], "FlightStats Configuration is OK"),
                self.checkDataSet( steps[2]["args"] ),
                self.checkDataSet( steps[3]["args"] ),
                self.checkLabeledRDD( LabeledRDDTrainingVarName, pixiedust_flightpredict.Configuration.TrainingSQLTableName ),
                self.checkLabeledRDD( LabeledRDDTestVarName, pixiedust_flightpredict.Configuration.TestSQLTableName  ),
                self.checkCustomHandlers(),
                self.checkModels()
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

    def checkCustomHandlers(self):
        customHandlers = [x for x in ShellAccess if isinstance(ShellAccess[x], defaultTrainingHandler)]
        return { "status-class": ICON_INFO,
                 "task": "{0} custom Handler found: {1}".format(len(customHandlers), reduce(lambda x,y: x + " " + y, customHandlers, "")),
                 "action": "Create a new custom Handler",
                 "code": "get_ipython().set_next_input({0})".format(self.loadTemplate("gencode/customHandlers.template")),
                 "id": "customHandlers"
                }

    def checkModels(self):
        models = pixiedust_flightpredict.Configuration.getModels()
        models = models if len(models)==0 else zip(*models)[0]
        return {
            "status-class": ICON_INFO,
            "task": "{0} model(s) have been created. {1}".format(len(models), reduce(lambda x,y: x + ", " + y, models, "")),
            "action": "Run the cells that build your models"
        }

    def checkLabeledRDD(self, varName, sqlTableName):
        if ShellAccess[varName] is None:
            return { "status-class": ICON_FAILED,   
                    "task": "The LabeledRDD {0} is not defined. If you already have a cell that loads it, please run it now. If not, click on the action button to generate a new cell".format(varName),
                    "action": "Generate Cell code to load {0}".format(varName),
                    "code": "get_ipython().set_next_input({0})".format(self.loadTemplate("gencode/labeledRDD.template", varName=varName, sqlTableName = sqlTableName)),
                    "id": varName
                    }

        return { "status-class": ICON_SUCCESS,
                 "task": "LabeledRDD {0} correctly loaded".format(varName),
                 "action": "None"
                }

    def checkDataSet(self, fieldInfos):
        task = self.checkConfigParams([f[1] for f in fieldInfos])
        if task["status-class"] == ICON_FAILED:
            return task
        
        DbName = pixiedust_flightpredict.Configuration[fieldInfos[0][1]]
        SQLTableVarName = pixiedust_flightpredict.Configuration[fieldInfos[1][1]]
        DFVarName = pixiedust_flightpredict.Configuration[fieldInfos[2][1]]

        if ShellAccess[DFVarName] is None:
            code = """ \\"dbName='{dbName}'\\\\n{varName} = pixiedust_flightpredict.loadDataSet(dbName,'{sqlTableName}')\\\\ndisplay({varName})\\" """\
                .format(varName=DFVarName,dbName=DbName, sqlTableName=SQLTableVarName)
            return { "status-class": ICON_FAILED,   
                    "task": "The variable {0} is not defined. If you already have a cell that loads it, please run it now. If not, click on the action button to generate a new cell".format(DFVarName),
                    "action": "Generate Cell code to load {0}".format(DFVarName),
                    "code": "get_ipython().set_next_input({0})".format(code),
                    "id": DFVarName
                    }

        return { "status-class": ICON_SUCCESS,
                 "task": "dataframe {0} correctly loaded".format(DFVarName),
                 "action": "None"
                }
