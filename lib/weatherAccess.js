//-------------------------------------------------------------------------------
// Copyright IBM Corp. 2015
//
// Licensed under the Apache License, Version 2.0 (the 'License');
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an 'AS IS' BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//-------------------------------------------------------------------------------

'use strict';

/**
 * Access to Weather API, include cache manager
 * 
 * @author David Taieb
 */
var request = require("request");
var _ = require("lodash");
var bluemixHelperConfig = require('bluemix-helper-config');
var configManager = bluemixHelperConfig.configManager;
var vcapServices = bluemixHelperConfig.vcapServices;

var weatherService = vcapServices.getService( configManager.get("WEATHER") || "weather" );

function weatherAccess(){
	var observationsMap = {};	//Cache values
	
	if ( !weatherService){
		var msg = "Unable to find Insight for Weather Service";
		console.log( msg );
		throw new Error( msg );
	}
	
	this.fetchObservations = function(airportInfo, callback){
		//Check the cache first
		if (observationsMap.hasOwnProperty( airportInfo.fs ) ){
			return callback(null, observationsMap[airportInfo.fs] );
		}
		
		// weather insights
		// var url = weatherService.credentials.url+"/api/weather/v2/observations/timeseries/24hour?units=m&geocode=" + 
		// 	encodeURIComponent(airportInfo.latitude.toFixed(2) + "," + airportInfo.longitude.toFixed(2)) +
		// 	"&language=en-US";

		// weather company data
		var url = weatherService.credentials.url + '/api/weather/v1/geocode/' +
			encodeURIComponent(airportInfo.latitude.toFixed(2)) + '/' +
			encodeURIComponent(airportInfo.longitude.toFixed(2)) + '/observations/timeseries.json?hours=23&language=en-US&units=m'

		request.get(url, {"json":true},
			function(err, response, body){
				if ( err || body.error || response.statusCode != 200 ){
					var error = err || body.error || ( "Code: " + response.statusCode + " : " + response.statusMessage )
					console.log("Unable to get weather timeSeries info for url %s ", url, error);
					return callback(error);
				}
				
				if ( body.errors ){
					console.log("Operational error returned by the weather service for url %s", url, body.errors);
					return callback("No weather data for url "+url);
				}
					
				//Collect all the observations
				var observations = body.observation || body.observations;
				if ( !_.isArray(observations) ){
					// return callback("Invalid results from weather api: "+body, body);
					observations = [observations]
				}
				observationsMap[airportInfo.fs] = observations;
				return callback(null, observations );
			}
		);
	}
}

module.exports = weatherAccess;