/**
 * @fileOverview Functions for querying and digesting AQUARIUS Web
 *               services.
 *
 * @author <a href="mailto:ashalper@usgs.gov">Andrew Halper</a>
 */

'use strict';

var http = require('http');
var querystring = require('querystring');

var aquarius = module.exports = {
    /**
       @function Call AQUARIUS GetLocationData Web service.
       @param {string} token AQUARIUS authentication token.
       @param {string} locationIdentifier AQUARIUS location identifier.
       @param {function} callback Callback function to call if/when
              response from GetLocationData is received.
    */
    getLocationData: function (hostname, token, locationIdentifier, callback) {
        /**
           @description Handle response from GetLocationData.
           @callback
        */
        function getLocationDataCallback(response) {
            var messageBody = "";

            // accumulate response
            response.on(
                "data",
                function (chunk) {
                    messageBody += chunk;
                });

            response.on("end", function () {
                callback(null, messageBody);
                return;
            });
        }
        
        var path = "/AQUARIUS/Publish/V2/GetLocationData?" +
            querystring.stringify(
                {token: token, format: "json",
                 LocationIdentifier: locationIdentifier}
            );

        var request = http.request({
            host: hostname,
            path: path                
        }, getLocationDataCallback);

        /**
           @description Handle GetTimeSeriesDescriptionList service
                        invocation errors.
        */
        request.on("error", function (error) {
            callback(error);
            return;
        });

        request.end();
    }, // getLocationData

    /**
       @function Call AQUARIUS GetTimeSeriesCorrectedData Web service.
       @param {string} token AQUARIUS authentication token.
       @param {string} timeSeriesUniqueId AQUARIUS
              GetTimeSeriesCorrectedData service
              TimeSeriesDataCorrectedServiceRequest.TimeSeriesUniqueId
              parameter.
       @param {string} queryFrom AQUARIUS GetTimeSeriesCorrectedData
              service TimeSeriesDataCorrectedServiceRequest.QueryFrom
              parameter.
       @param {string} QueryTo AQUARIUS GetTimeSeriesCorrectedData
              service TimeSeriesDataCorrectedServiceRequest.QueryTo
              parameter.
       @param {function} callback Callback to call if/when
              GetTimeSeriesCorrectedData service responds.
    */
    getTimeSeriesCorrectedData: function (
        aquariusHostname, token, timeSeriesUniqueId, queryFrom,
        queryTo, applyRounding, callback
    ) {
        /**
           @description Handle response from GetTimeSeriesCorrectedData.
           @callback
        */
        function getTimeSeriesCorrectedDataCallback(response) {
            var messageBody = "";
            var timeSeriesCorrectedData;

            // accumulate response
            response.on(
                "data",
                function (chunk) {
                    messageBody += chunk;
                });

            response.on("end", function () {
                callback(null, messageBody);
                return;
            });
        } // getTimeSeriesCorrectedDataCallback

        var path = "/AQUARIUS/Publish/V2/GetTimeSeriesCorrectedData?" +
            querystring.stringify(
                {token: token, format: "json",
                 TimeSeriesUniqueId: timeSeriesUniqueId,
                 QueryFrom: queryFrom, QueryTo: queryTo,
                 ApplyRounding: applyRounding.toString()}
            );

        var request = http.request({
            host: aquariusHostname,
            path: path
        }, getTimeSeriesCorrectedDataCallback);

        /**
           @description Handle GetTimeSeriesCorrectedData service
           invocation errors.
        */
        request.on("error", function (error) {
            callback(error);
            return;
        });

        request.end();
    }, // getTimeSeriesCorrectedData

    /**
       @function Parse AQUARIUS TimeSeriesDataServiceResponse received
                 from GetTimeSeriesCorrectedData service.
    */
    parseTimeSeriesDataServiceResponse: function (messageBody, callback) {
        var timeSeriesDataServiceResponse;

        try {
            timeSeriesDataServiceResponse = JSON.parse(messageBody);
        }
        catch (error) {
            callback(error);
            return;
        }

        callback(null, timeSeriesDataServiceResponse);
    }, // parsetimeSeriesDataServiceResponse

    /**
       @function Distill a set of time series descriptions into
                 (hopefully) one to query for a set of time series
                 date/value pairs.
       @param {object} timeSeriesDescriptions An array of AQUARIUS
              TimeSeriesDescription objects.
       @param {object} locationIdentifier A LocationIdentifier object.
       @param {function} callback Callback function to call if/when
              one-and-only-one candidate TimeSeriesDescription object
              is found, or, to call with node-async, raise error
              convention.
    */
    distill: function (timeSeriesDescriptions, locationIdentifier, callback) {
        var timeSeriesDescription;

        switch (timeSeriesDescriptions.length) {
        case 0:
            /**
               @todo error callback
            */
            break;
        case 1:
            timeSeriesDescription = timeSeriesDescriptions[0];
            break;
        default:
            /**
               @description Filter out set of primary time series.
            */
            async.filter(
                timeSeriesDescriptions,
                /**
                   @function Primary time series filter iterator function.
                   @callback
                */
                function (timeSeriesDescription, callback) {
                    /**
                       @description Detect
                       {"Name": "PRIMARY_FLAG",
                       "Value": "Primary"} in
                       TimeSeriesDescription.ExtendedAttributes
                    */
                    async.detect(
                        timeSeriesDescription.ExtendedAttributes,
                        /**
                           @function Primary time series, async.detect
                           truth value function.
                           @callback
                        */
                        function (extendedAttribute, callback) {
                            // if this time series description is
                            // (hopefully) the (only) primary one
                            if (extendedAttribute.Name === "PRIMARY_FLAG"
                                &&
                                extendedAttribute.Value === "Primary") {
                                callback(true);
                            }
                            else {
                                callback(false);
                            }
                        },
                        /**
                           @function Primary time series, async.detect
                                     final function.
                           @callback
                        */
                        function (result) {
                            // notify async.filter that we...
                            if (result === undefined) {
                                // ...did not find a primary time series
                                // description
                                callback(false);
                            }
                            else {
                                // ...found a primary time series
                                // description
                                callback(true);
                            }
                        }
                    );
                },
                /**
                   @function Check arity of primary time series
                             descriptions returned from AQUARIUS
                             GetTimeSeriesDescriptionList.
                   @callback
                */
                function (primaryTimeSeriesDescriptions) {
                    // if there is 1-and-only-1 primary time
                    // series description
                    if (primaryTimeSeriesDescriptions.length === 1) {
                        timeSeriesDescription = timeSeriesDescriptions[0];
                    }
                    else {
                        /**
                           @todo We should probably defer production of
                           header and heading until after this
                           check.
                        */
                        // raise error
                        callback(
                            'More than 1 primary time series found for "' +
                                locationIdentifier.toString() + '"'
                        );
                    }
                }
            ); // async.filter
        } // switch (timeSeriesDescriptions.length)

        return timeSeriesDescription;
    } // distill

} // aquarius
