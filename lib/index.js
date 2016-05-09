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

var request = require("request");
var _ = require("lodash");
var async = require("async");
var pipesSDK = require('simple-data-pipe-sdk');
var pipesDb = pipesSDK.pipesDb;
var connectorExt = pipesSDK.connectorExt;
var cloudant = pipesSDK.cloudant;
var moment = require("moment");
var flightAccess = require("./flightAccess");
var weatherAccess = require("./weatherAccess");

var concurrency = 20;	//Async queue concurrency
/**
 * Pipes Connector for flightStats (part of flight predictor app)
 */
function flightStatsConnector( parentDirPath ){
	//Call constructor from super class
	connectorExt.call(this, "flightStats", "Flight Stats", {
		copyToDashDb: false,
		extraRequiredFields: null,
		useOAuth: false,
		useCustomTables: true,
		recreateTargetDb: false	//Preserve target db as we want to accumulate training data
	});
	
	this.getTablePrefix = function(){
		return "flightStats";
	}
	
	this.getCloudantDbName = function(pipe, table){
		return pipe.name + "_" + table.name;
	}
	
	var designFlightMetadataName = '_design/flightMetadata';
	var USAirportViewName = 'US Airports';
	var AllAirportsViewName = "airports";
	var AllAirlinesViewName = "airlines";
	var genViewsForFlightMetadata = function(){
		var manager = new cloudant.views( designFlightMetadataName );
		
		manager.addView(
			AllAirportsViewName,
			JSON.parse("{"+
					"\"map\": \"function(doc){" +
					"if ( doc.metadata_type === '" + AllAirportsViewName + "' && doc.fs != null){" +
					"emit( doc.fs, {'_id': doc._id, 'rev': doc._rev, 'name':doc.name } );" +
					"}" +
					"}\"" +
					"}"
			), 2 //Version
		)
		.addView(
			AllAirlinesViewName,
			JSON.parse("{"+
					"\"map\": \"function(doc){" +
					"if ( doc.metadata_type === '" + AllAirlinesViewName + "'){" +
					"emit( doc._id, {'_id': doc._id, 'rev': doc._rev } );" +
					"}" +
					"}\"" +
					"}"
			), 2 //Version
		)
		.addView(
			USAirportViewName,
			JSON.parse("{"+
				"\"map\": \"function(doc){" +
           			"if ( doc.metadata_type === '" + AllAirportsViewName + "' && doc.countryCode==='US' && doc.fs != null){" +
           				"emit( doc.fs, {'_id': doc._id, 'rev': doc._rev, 'name':doc.name } );" +
           			"}" +
       			"}\""+
			"}"
			), 3 //Version
		);

		return [manager];
	}
	
	var metadataDb = null;
	var loadMetadataDb = function(callback,appId, appKey){
		if ( metadataDb ){
			return callback();
		}
		
		//Check if the flightMetadata database exists
		var dbName = "flight-metadata";
		metadataDb = new cloudant.db(dbName, genViewsForFlightMetadata() );
		metadataDb.on( "cloudant_ready", function(){
			if ( metadataDb.dbCreated ){
				//Load the airlines and airports info
				console.log("Loading airlines and airports info");
				
				if ( !appId || !appKey ){
					return callback("Unable to load flight metadata db: No appId or appKey provided");
				}
				
				async.parallel([flightAccess.loadAirports.bind(metadataDb, appId, appKey), flightAccess.loadAirlines.bind(metadataDb, appId, appKey)],function(err, results){
					if ( err ){
						metadataDb = null;
					}
					
					return callback(err);
				});
			}else{
				return callback();
			}
		});
	
		metadataDb.on("cloudant_error", function(){
			var message = "Fatal error from Cloudant database: unable to initialize " + dbName;
			console.log( message );
			metadataDb = null;
			return callback( message );
		});
	}
	
	var getAirportInfo = function( airportFsCode, callback ){
		loadMetadataDb( function(err){
			if ( err ){
				return callback(err);
			}
			metadataDb.run( function(err, db){
				if ( err ){
					return callback(err);
				}
				db.view('flightMetadata', AllAirportsViewName, {key: airportFsCode, include_docs: true}, function(err, body){
					if ( err || body.rows.length <= 0){
						return callback(err || "Unable to find record for airport " + airportFsCode);
					}
					return callback( null, body.rows[0].doc );
				});
			})
		});
	}
	
	var superConnectDataSource = this.connectDataSource;
	this.connectDataSource = function( req, res, pipeId, url, callback ){
		pipesDb.getPipe( pipeId, function( err, pipe ){
			if ( err ){
				return callback(err);
			}
			loadMetadataDb(function(err){
				if ( err ){
					return callback(err);
				}
				return superConnectDataSource.call(this, req, res, pipeId, url, callback);
			}.bind(this),pipe.appId, pipe.appKey);
		}.bind(this));
	};
	
	/**
	 * getTables: return Array of table objects: {name:'XXXX', labelPlural: 'XXXX}
	 */
	var busiestAirports = ['ATL','LAX','ORD','DFW','JFK','DEN','SFO','CLT','LAS','PHX','IAH','MIA','SEA','EWR','MCO','MSP','DTW',
	                       'BOS','PHL','LGA','FLL','BWI','IAD','MDW','SLC','DCA','HNL','SAN','TPA','PDX'];
	this.getTables = function(){
		return [
	        {name : null, labelPlural : 'All Sets'},
	        {name : 'mega_set', labelPlural : 'Mega Set from 30 busiest airports', CLIENT_airportCodes:busiestAirports},
	        {name : 'training_set', labelPlural : 'Training Set', CLIENT_airportCodes:["BOS","SFO","MIA","AUS", "ORD"]},
	        {name : 'test_set', labelPlural : 'Test Set', CLIENT_airportCodes: ["LAS"]},
	        {name : 'blind_set', labelPlural : 'Blind Set', CLIENT_airportCodes: ["SEA"]}
        ];
	}
	
	/**
	 * runStarted: lifecycle event called when a new run is started for this connector
	 */
	this.runStarted = function(readyCallback, pipe){
		console.log("Run Started, creating a new weatherObservations object");
		this.weatherObservations = new weatherAccess();
		
		return loadMetadataDb(readyCallback, pipe.appId, pipe.appKey);
	}
	
	/**
	 * runStarted: lifecycle event called when a new run is finished for this connector
	 */
	this.runFinished = function(pipe){
		console.log("Run Finished, deleting weatherObservations object");
		if ( this.weatherObservations ){
			delete this.weatherObservations;
			this.weatherObservations = null;
		}
	}

	this.fetchRecords = function( table, pushRecordFn, done, pipeRunStep, pipeRunStats, logger, pipe, pipeRunner ){		
		console.log("Processing table: ", table);
		
		var airportCodes = table.CLIENT_airportCodes;
		if ( !airportCodes || !airportCodes.length){
			return done("No airport code specified for this run");
		}
		
		var appId = pipe.appId;
		var appKey = pipe.appKey;
		
		var weatherObservations = this.weatherObservations;
		async.waterfall([
         function(callback){
			 loadMetadataDb(function(err){
				 if ( err ){
					 return callback(err);
				 }
				 return callback(null, metadataDb);
			 },appId, appKey )
		 },
         function(metadataDb, callback){
			 async.map( airportCodes, getAirportInfo, callback );
         },
         function(airportInfos, callback ){
        	 //Fetch weather observations
        	 async.map(airportInfos, function(airportInfo, callback){
        		 weatherObservations.fetchObservations( airportInfo, function(err, observations ){
        			 if(err){
        				 return callback(err);
        			 }
        			 airportInfo.observations=observations;
        			 var map = {};
        			 var flightDates = [];
        			 _.forEach( observations, function(observation){
						var m = moment.unix(observation.valid_time_gmt);
						var date = {
								year: m.year(),
								month: m.month() + 1,
								day : m.date(),
								hour : m.hour()
						};
						var key = date.year + "" + date.month + "" + date.day + "" + date.hour;
						if (!map.hasOwnProperty(key)){
							map[key]=1;
							flightDates.push(date);
						}
        			 });
        			 airportInfo.flightDates = flightDates;
        			 
        			 //Fetch flight schedules for each flight dates
        			 airportInfo.cursorIndex=0;
        			 airportInfo.flights=[];
        			 async.each( flightDates, function(date, callback){
        				flightAccess.getFlights( appId, appKey, airportInfo, date, function(err, flights){
        					if(err){
        						return callback(err);
        					}
        					_.forEach(flights, function(flight){
        						//console.log("flightddd: ",flight)
        						airportInfo.flights.push(flight);
        					});        					
        					return callback();
        				});
        			 }, function( err ){
        				 if ( err ){
        					 console.log("An error occurred while processing flights schedules", err);
        					 return callback(err);
        				 }
        				 return callback(null, airportInfo);
					 });
				});
        	 },callback);
         },
         function( airportInfos, callback ){
        	 //console.log("airportInfos: ", airportInfos);
        	 var q = async.queue( function(task, callback){
        		 var flight=task.flight;
        		 var airportInfo=task.airportInfo;
				//Get arrival airport info and weather observations
				getAirportInfo(flight.arrivalAirportFsCode, function(err, arrivalAirportInfo ){
					if ( err ){
						return callback(err);
					}
					weatherObservations.fetchObservations( arrivalAirportInfo, function(err, arrivalObservations ){
						if ( err ){
							return callback(err);
						}
						var airportData = {
							depAirportInfo : airportInfo,
							depObservations : airportInfo.observations,
							arrivalAirportInfo : arrivalAirportInfo,
							arrivalObservations : arrivalObservations
						};
						return flightAccess.addStatusAndWeatherInfo(appId, appKey, flight, date, airportData, pushRecordFn, callback );
					});
				});
        	 }, concurrency);
        	 
        	 //Since Flight Data Source allows limited number of api calls, we want to distribute the calls fairly across all airports
        	 var more = true;
        	 while(more){
        		 more=false;
        		 _.forEach(airportInfos, function(airportInfo){
        			 if(airportInfo.cursorIndex < airportInfo.flights.length){
        				 more=true;
        				 var flight = airportInfo.flights[airportInfo.cursorIndex++];
        				 q.push( {flight:flight,airportInfo:airportInfo}, function( err ){
        					 if ( err ){
        						 console.log("Error processing flight record ", err);
        					 }
        				 });
        			 }
        		 });
        	 }
        	 
        	 //Add a drain function
        	 q.drain = function(){
				return callback();
        	 }
        	 
        	 //async.eachSeries( airportInfos, generateDataForAirport, callback );
         }
        ], function(err, results){
			if ( err ){
				console.log("An error occurred while processing airports", err);
			}
			return done(err);
		});
		
		var generateDataForAirport = function(airportInfo, callback){
			console.log("Generating data for airport ", airportInfo.name);
			
			//Get the observations for the past 24 hours, use the results to search for flight schedule so that we always have weather data for each flight records
			weatherObservations.fetchObservations( airportInfo, function(err, observations ){
				if ( err ){
					return callback(err);
				}
				var map = {};
				var flightDates = [];
				_.forEach( observations, function(observation){
					var m = moment.unix(observation.valid_time_gmt);
					var date = {
							year: m.year(),
							month: m.month() + 1,
							day : m.date(),
							hour : m.hour()
					};
					var key = date.year + "" + date.month + "" + date.day + "" + date.hour;
					if (!map.hasOwnProperty(key)){
						map[key]=1;
						flightDates.push(date);
					}
				});

				//Process each dates for this airport
				console.log(flightDates);

				async.each( flightDates, function(date, callback){
					processDate( date, observations, airportInfo, callback );
				}, function( err ){
					if ( err ){
						console.log("An error occurred while processing flights schedules", err);
					}
					return callback(err);
				});
			});
		}
		
		var mockRequest = {
			get: function(url, params, callback ){
				return callback(null, {}, {
					scheduledFlights:[
	                   {
	                	   "flightNumber": "1024",
	                	   "departureAirportFsCode": "BOS",
	                	   "carrierFsCode": "WN",
	                	   "arrivalAirportFsCode": "BWI",
	                	   "departureTime": "2015-12-29T12:30:00.000",
	                	   "arrivalTime": "2015-12-29T14:00:00.000",
	                	   "status": "L"
	        		   }
	                ]
				});
			}
		}
		//request = mockRequest;
		
		var processDate = function( date, observations, airportInfo, callback ){
			var connectedUrl = flightAccess.buildUrl(appId, appKey,
				"schedules/rest/v1/json/from/" + airportInfo.fs + "/departing/" + date.year + "/" + date.month + "/" + date.day + "/" + date.hour
			);
			console.log("fetching flight schedules for " + airportInfo.name, connectedUrl);
			request.get(connectedUrl, {"json": true},
				function( err, response, body ){
					if ( err || body.error){
						console.log("Unable to fetch flight schedules: ", err || body.error );
						return callback( err );
					}
					
					var flights = body.scheduledFlights;
					if ( !_.isArray(flights) ){
						return callback("Invalid results from scheduled api: ", body);
					}
					
					if ( flights.length == 0 ){
						console.log("No flights scheduled departing from airport " + airportInfo.fs + " on " + date.year + "/" + date.month + "/" + date.day + " at " + date.hour);
						return callback();
					}
					console.log("Processing " + flights.length + " flights for date ", date );
					var q = async.queue( function(flight, callback){
						//Get arrival airport info and weather observations
						getAirportInfo(flight.arrivalAirportFsCode, function(err, arrivalAirportInfo ){
							if ( err ){
								return callback(err);
							}
							weatherObservations.fetchObservations( arrivalAirportInfo, function(err, arrivalObservations ){
								if ( err ){
									return callback(err);
								}
								var airportData = {
									depAirportInfo : airportInfo,
									depObservations : observations,
									arrivalAirportInfo : arrivalAirportInfo,
									arrivalObservations : arrivalObservations
								};
								return flightAccess.addStatusAndWeatherInfo(appId, appKey, flight, date, airportData, pushRecordFn, callback );
							});
						});
					}, concurrency);					
					q.push( flights, function( err ){
						if ( err ){
							console.log("Error processing flight record ", err);
							//q.kill();
							//return callback(err);
						}
					});
					
					//Add a drain function
					q.drain = function(){
						return callback();
					}
				}
			);
		}
	};
}

//Extend event Emitter
require('util').inherits(flightStatsConnector, connectorExt);

module.exports = new flightStatsConnector();