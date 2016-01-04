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
 * Access to flightStats API
 * 
 * @author David Taieb
 */
var request = require("request");
var _ = require("lodash");
var bluemixHelperConfig = require('bluemix-helper-config');
var configManager = bluemixHelperConfig.configManager;
var moment = require("moment");

var appId = configManager.get("APP_ID");
var appKey = configManager.get("APP_KEY");
var flexBaseUrl = "https://api.flightstats.com/flex/";

var buildUrl = module.exports.buildUrl = function( path, params ){
	var retUrl = flexBaseUrl + path + "?appId=" + appId + "&appKey=" + appKey;
	if ( params ){
		_.forEach(params, function( value, key){
			retUrl += "&" + key + "=" + value;
		});
	}
	return retUrl;
}

var getClosestObservation = function(observations, scheduledMoment){
	var closestObservation = null;
	var min = 0;
	_.forEach(observations, function(observation){
		if ( !observation.m ){
			observation.m = moment.unix(observation.valid_time_gmt);
		}
		var thisMin = Math.abs(scheduledMoment.diff( observation.m, 'minutes' ));

		if ( !closestObservation || thisMin < min ){
			closestObservation = observation;
			min = thisMin;
		}					
	})
	return 	closestObservation;
}

module.exports.addStatusAndWeatherInfo = function( flight, date, airportData, pushRecordFn, callback ){
	var depAirportInfo = airportData.depAirportInfo;
	var depObservations = airportData.depObservations;
	var arrivalAirportInfo = airportData.arrivalAirportInfo;
	var arrivalObservations = airportData.arrivalObservations;
	
	//Get flight status
	var statusUrl = buildUrl( "flightstatus/rest/v2/json/flight/status/" 
		+ flight.carrierFsCode + "/" + flight.flightNumber + "/dep/" + date.year + "/" + date.month + "/" + date.day,{
			"airport": depAirportInfo.fs
		});
	console.log("Getting flight status with url: ", statusUrl);
	request.get( statusUrl, {"json":true},
		function(err, response, body){
			if ( err || body.error || response.statusCode != 200 ){
				console.log("Unable to get status for flight: ", err || body.error || ( "Code: " + response.statusCode + " : " + response.statusMessage ) );
				return callback(err);
			}
			//Augment the flight info with the actual status and push it to the pipe
			var flightStatuses = body.flightStatuses;
			if ( flightStatuses.length > 0 ){
				var flightStatus = flightStatuses[0];
				flight.status = flightStatus.status;
				flight.scheduledGateDeparture = flightStatus.operationalTimes.scheduledGateDeparture;
				flight.actualRunwayDeparture = flightStatus.operationalTimes.actualRunwayDeparture;
				flight.actualRunwayArrival = flightStatus.operationalTimes.actualRunwayArrival;
				//delta in minutes between actual and scheduled, make sure that the flight has already landed
				if ( flight.scheduledGateDeparture && flight.actualRunwayDeparture && flight.actualRunwayArrival ){
					var scheduledDeparture = moment.utc(flight.scheduledGateDeparture.dateUtc);
					var actualDeparture = moment.utc(flight.actualRunwayDeparture.dateUtc);
					var actualArrival = moment.utc(flight.actualRunwayArrival.dateUtc);
					flight.deltaDeparture = actualDeparture.diff( scheduledDeparture, 'minutes' );
					
					if ( flight.status === "C"){
						flight.classification = 0;
						flight.classificationMsg = "Canceled";
					}else if ( flight.deltaDeparture < 15 ){
						flight.classification = 1;
						flight.classificationMsg = "On Time";
					}else if ( flight.deltaDeparture < 120 ){
						flight.classification = 2;
						flight.classificationMsg = "Delay less 2 hours";
					}else if ( flight.deltaDepartur < 240 ){
						flight.classification = 3;
						flight.classificationMsg = "Delay between 2 and 4 hours";
					}else{
						flight.classification = 4;
						flight.classificationMsg = "Delay more than 4 hours";
					}
					
					//Add weather observations for both departure and arrival airport
					flight.departureWeather = getClosestObservation( depObservations, scheduledDeparture );
					flight.arrivalWeather = getClosestObservation( arrivalObservations, actualArrival );
					
					pushRecordFn(flight);
				}
			}else{
				console.log("Status payload does not have flightStatuses field ", body)
			}
			return callback();							
		}
	);
}

module.exports.loadAirlines = function( callback ){
	var targetDb = this;
	var connectedUrl = buildUrl("airlines/rest/v1/json/active");
	console.log("Connecting to airline flight stats with url " + connectedUrl );
	request.get(connectedUrl, {"json": true},
		function( err, response, body ){
			if ( err || body.error){
				console.log("Unable to fetch list of active airports: ", err || body.error );
				return callback( err );
			}
			var airlines = body.airlines;
			if ( !_.isArray(airlines) ){
				return callback("Invalid results from airlines api: ", body);
			}
			
			//Insert each airport in batch of 200 records
			var numAirlinesCopied = 0;
			var q = async.queue( function( batch, callback ){
				targetDb.run( function( err, db ){
					if ( err || !batch.batch){
						return callback(err);
					}
					//Update docs in bulks
					db.bulk( {"docs": batch.batch}, function( err, data ){
						if ( err ){
							console.log("Error calling bulk: ", err, data)
							return callback(err);
						}
						numAirlinesCopied += batch.batch.length;
						return callback();
					});
				});
			}, concurrency);
			
			async.forEach( _.chunk(airlines, 100 ), function( batch ){
				_.forEach(batch, function(airline){
					airline.metadata_type = 'airlines';
				});
				q.push( {batch: batch}, function(err){
					if ( err ){
						console.log("Error processing airline record ", err);
						q.kill();
					}
				});
			});
			
			//Add a drain function
			q.drain = function(){
				console.log("Successfully copied " + numAirlinesCopied + " airline records");
				return callback();
			}
		}
	);
}

module.exports.loadAirports = function( callback ){
	var targetDb = this;
	var connectedUrl = buildUrl("airports/rest/v1/json/active");
	console.log("Connecting to airports flight stats with url " + connectedUrl );
	request.get(connectedUrl, {"json": true},
		function( err, response, body ){
			if ( err || body.error){
				console.log("Unable to fetch list of active airports: ", err || body.error );
				return callback( err );
			}
			console.log("Successfully connected to flightstats airports API");
			var airports = body.airports;
			if ( !_.isArray(airports) ){
				return callback("Invalid results from airports api: ", body);
			}

			var numAirportsCopied = 0;
			var q = async.queue( function( batch, callback ){
				targetDb.run( function( err, db ){
					if ( err || !batch.batch){
						return callback(err);
					}
					//Update docs in bulks
					db.bulk( {"docs": batch.batch}, function( err, data ){
						if ( err ){
							console.log("Error calling bulk: ", err, data)
							return callback(err);
						}
						numAirportsCopied += batch.batch.length;
						return callback();
					});
				});
			}, concurrency);
			
			//Insert each airport in batch of 200 records
			async.forEach( _.chunk(airports, 200 ), function( batch ){
				_.forEach(batch, function(airport){
					airport.metadata_type = 'airports';
				});
				q.push( {batch: batch}, function(err){
					if ( err ){
						console.log("Error processing airport record ", err);
						q.kill();
					}
				});
			});
			
			//Add a drain function
			q.drain = function(){
				console.log("Successfully copied " + numAirportsCopied + " airport records");
				return callback();
			}
		}
	);
}
