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
