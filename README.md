
# Simple Data Pipe Connector for flightstats.com

[Simple Data Pipe](https://developer.ibm.com/clouddataservices/simple-data-pipe/) connector for [flighstats.com](http://www.flightstats.com/). This connector generates training, test and blind data for the flight predictor spark MLLib application.

### Pre-requisites

##### General 

To load flightstats.com data you have to have a <a href="https://developer.flightstats.com/">developer account</a>. A 30 day trial is available for evaluation purposes.

##### Deploy the Simple Data Pipe

  [Deploy the Simple Data Pipe in Bluemix](https://github.com/ibm-cds-labs/simple-data-pipe) using the Deploy to Bluemix button or manually.

##### Services

This connector requires the [Insights for Weather service](https://console.ng.bluemix.net/catalog/services/insights-for-weather/) in IBM Bluemix to be bound to the Simple Data Pipe application. 

To provision and bind an Insights for Weather service instance in Bluemix run the follow commands:

````
  $ cf create-service weatherinsights Free weather
  $ cf bind-service simple-data-pipe weather
  $ cf restage simple-data-pipe
````

> Pro Tip: If you want to re-use an existing instance that is not named `weather`, create a [USER-DEFINED Environment Variable](https://www.ng.bluemix.net/docs/manageapps/depapps.html#ud_env) in the Simple Data Pipe application named __WEATHER__ and set its value to the name of the existing Insights for Weather service.

##### Install the flightstats.com connector

Install the connector using [these instructions](https://github.com/ibm-cds-labs/pipes/wiki/Installing-a-Simple-Data-Pipe-Connector) into the Simple Data Pipe. 

##### Enable OAuth support and collect connectivity information

Before the Simple Data Pipe can connect to flightstats.com, complete these steps:

1. Log in to the [flightstats.com developer center](https://developer.flightstats.com).
2. Open the  _Dashboard_. 
3. Take note of the __APPLICATION ID__ and __APPLICATION KEY__.

### Using the flightstats.com Connector 

1. Open the Simple Data Pipe web console.
2. Select __Create A New Pipe__.
3. Select __Flight Stats__ for the __Type__, provide a unique pipe __Name__ (e.g. my flightstats demo pipe) and an optional __Description__.
4. In the _Connect_ page, fill in the _Application ID_ and the _Application Key_ from the flightstats developer center.
5. Select which data sets to retrieve: __all__, __training__, __test__ or __blind__. 
6. Schedule a pipe run or run the pipe immediately.

#### License 

Copyright [2016] IBM Cloud Data Services

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
