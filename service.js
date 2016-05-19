/**
 * @fileOverview Prototypes for querying aq2rdb prerequisite Web
 *               services.
 *
 * @module service
 *
 * @author Andrew Halper <ashalper@usgs.gov>
 */

'use strict';

// Node.js modules
var async = require("async");
var http = require("http");
var https = require('https');
var fs = require("fs");
var querystring = require("querystring");

// aq2rdb modules
var rest = require("./rest");

var service = module.exports = {

/**
   @class
   @classdesc AQUARIUS object prototype.
   @public
   @param {string} aquariusTokenHostname DNS host name of
                   aquarius-token server.
   @param {string} hostname DNS host name of AQUARIUS server.
   @param {string} userName AQUARIUS account user name.
   @param {string} password AQUARIUS account password.
   @param {function} callback Callback to call when object
          constructed.
*/
AQUARIUS: function (
    aquariusTokenHostname, hostname, userName, password, callback
) {
    var aquariusTokenHostname, token, remarkCodes;
    var port = "8080";
    var path = "/services/GetAQToken?";
    var uriString = "http://" + hostname + "/AQUARIUS/";

    if (aquariusTokenHostname === undefined) {
        callback('Required field "aquariusTokenHostname" not found');
        return;
    }

    if (aquariusTokenHostname === '') {
        callback('Required field "aquariusTokenHostname" must have a value');
        return;
    }

    aquariusTokenHostname = aquariusTokenHostname;

    if (hostname === undefined) {
        callback('Required field "hostname" not found');
        return;
    }

    if (hostname === '') {
        callback('Required field "hostname" must have a value');
        return;
    }

    this.hostname = hostname;

    if (userName === undefined) {
        callback('Required field "userName" not found');
        return;
    }

    if (userName === '') {
        callback('Required field "userName" must have a value');
        return;
    }

    if (password === undefined) {
        callback('Required field "password" not found');
        return;
    }

    if (password === '') {
        callback('Required field "password" must have a value');
        return;
    }

    /**
       @function
       @description GetAQToken service response callback.
       @private
       @callback
    */
    function getAQTokenCallback(response) {
        var messageBody = '';

        // accumulate response
        response.on('data', function (chunk) {
            messageBody += chunk;
        });

        // Response complete; token received.
        response.on('end', function () {
            token = messageBody;
            callback(
                null,
                "Received AQUARIUS authentication token successfully"
            );
            return;
        });
    } // getAQTokenCallback

    // make sure to not reveal user-name/passwords in log
    path += querystring.stringify(
        {userName: userName, password: password,
         uriString: uriString}
    );

    /**
       @description GetAQToken service request for AQUARIUS
                    authentication token needed for AQUARIUS API.
    */
    var request = http.request({
        host: aquariusTokenHostname,
        port: port,             // TODO: make a CLI parameter?
        path: path
    }, getAQTokenCallback);

    /**
       @description Handle GetAQToken service invocation errors.
       @callback
    */
    request.on('error', function (error) {
        var statusMessage;

        if (error.message === 'connect ECONNREFUSED') {
            callback("Could not connect to GetAQToken service for " +
                     "AQUARIUS authentication token");
        }
        else {
            callback(error);
        }
        return;
    });

    request.end();

    /**
       @method
       @description AQUARIUS authentication token accessor method.
     */
    this.token = function () {
        return token;
    }

    /**
       @method
       @description Call AQUARIUS GetLocationData Web service.
       @param {string} locationIdentifier AQUARIUS location identifier.
       @param {function} callback Callback function to call if/when
              response from GetLocationData is received.
    */
    this.getLocationData = function (locationIdentifier, callback) {
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
           @description Handle GetLocationData service invocation
                        errors.
        */
        request.on("error", function (error) {
            callback(error);
            return;
        });

        request.end();
    } // getLocationData

    /**
       @method
       @description Call AQUARIUS GetTimeSeriesCorrectedData Web service.
       @param {object} parameters AQUARIUS
              GetTimeSeriesCorrectedData service HTTP parameters.
       @param {function} callback Callback to call if/when
              GetTimeSeriesCorrectedData service responds.
    */
    this.getTimeSeriesCorrectedData = function (parameters, callback) {
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

        // these parameters span every GetTimeSeriesCorrectedData
        // call for our purposes, so they're not passed in
        parameters["token"] = token;
        parameters["format"] = "json";

        var path = "/AQUARIUS/Publish/V2/GetTimeSeriesCorrectedData?" +
            querystring.stringify(parameters);

        var request = http.request({
            host: hostname,
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
    } // getTimeSeriesCorrectedData

    /**
       @method
       @description Parse AQUARIUS TimeSeriesDataServiceResponse
                    received from GetTimeSeriesCorrectedData service.
       @param {string} messageBody Message from AQUARIUS Web service.
       @param {function} callback Callback function to call when
                                  response is received.
    */
    this.parseTimeSeriesDataServiceResponse = function (
        messageBody, callback
    ) {
        var timeSeriesDataServiceResponse;

        try {
            timeSeriesDataServiceResponse = JSON.parse(messageBody);
        }
        catch (error) {
            callback(error);
            return;
        }

        callback(null, timeSeriesDataServiceResponse);
    } // parsetimeSeriesDataServiceResponse

    /**
       @method
       @public
       @description Cache remark codes.
    */
    this.getRemarkCodes = function () {
        // if remark codes have not been loaded yet
        if (remarkCodes === undefined) {
            // load them
            remarkCodes = new Object();

            async.waterfall([
                /**
                   @function
                   @description Request remark codes from AQUARIUS.
                   @callback
                */
                function (callback) {
                    try {
                        rest.query(
                            hostname,
                            "GET",
                            undefined,      // HTTP headers
                            "/AQUARIUS/Publish/V2/GetQualifierList/",
                            {token: token, format: "json"},
                            false,
                            callback
                        );
                    }
                    catch (error) {
                        callback(error);
                        return;
                    }
                },
                /**
                   @function
                   @description Receive remark codes from AQUARIUS.
                   @callback
                */
                function (messageBody, callback) {
                    var qualifierListServiceResponse;

                    try {
                        qualifierListServiceResponse =
                            JSON.parse(messageBody);
                    }
                    catch (error) {
                        callback(error);
                        return;
                    }

                    // if we didn't get the remark codes domain table
                    if (qualifierListServiceResponse === undefined) {
                        callback(
                            "Could not get remark codes from http://" +
                                hostname +
                                "/AQUARIUS/Publish/V2/GetQualifierList/"
                        );
                        return;
                    }

                    // put remark codes in an array for faster access later
                    remarkCodes = new Array();
                    async.each(
                        qualifierListServiceResponse.Qualifiers,
                        /** @callback */
                        function (qualifierMetadata, callback) {
                            remarkCodes[qualifierMetadata.Identifier] =
                                qualifierMetadata.Code;
                            callback(null);
                        }
                    );

                    callback(null);
                }
            ]);
        }
    } // getRemarkCodes

    /**
       @function
       @description Distill a set of time series descriptions into
                    (hopefully) one, to query for a set of time series
                    date/value pairs.
       @private
       @param {object} timeSeriesDescriptions An array of AQUARIUS
              TimeSeriesDescription objects.
       @param {object} locationIdentifier A LocationIdentifier object.
       @param {function} callback Callback function to call if/when
              one-and-only-one candidate TimeSeriesDescription object
              is found, or, to call with node-async, raise error
              convention.
    */
    function distill(
        timeSeriesDescriptions, locationIdentifier, callback
    ) {
        var timeSeriesDescription;

        switch (timeSeriesDescriptions.length) {
        case 0:
            callback(
                "No time series descriptions found for LocationIdentifier \"" +
                    locationIdentifier + "\""
            );
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
                   @function
                   @description Primary time series filter iterator
                                function.
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
                           @function
                           @description Primary time series,
                                        async.detect truth value
                                        function.
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
                           @function
                           @description Primary time series,
                                        async.detect final function.
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
                   @function
                   @description Check arity of primary time series
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
                        // raise error
                        var error =
                            "More than one primary time series found for \"" +
                            locationIdentifier.toString() + "\":\n" +
                            "#\n";
                        async.each(
                            primaryTimeSeriesDescriptions,
                            /** @callback */
                            function (desc, callback) {
                                error += "#   " + desc.Identifier + "\n";
                                callback(null);
                            }
                        );
                        callback(error);
                    }
                }
            ); // async.filter
        } // switch (timeSeriesDescriptions.length)

        callback(null, timeSeriesDescription);
    } // distill

    /**
       @function
       @description Query AQUARIUS GetTimeSeriesDescriptionList
                    service to get list of AQUARIUS, time series
                    UniqueIds related to aq2rdb, location and
                    parameter.
       @private
       @param {string} agencyCode USGS agency code.
       @param {string} siteNumber USGS site number.
       @param {string} parameter AQUARIUS parameter.
       @param {string} computationIdentifier AQUARIUS computation
                       identifier.
       @param {string} computationPeriodIdentifier AQUARIUS
                       computation period identifier.
       @param {function} callback async.waterfall() callback
              function.
    */
    function getTimeSeriesDescriptionList(
        agencyCode, siteNumber, parameter, computationIdentifier,
        computationPeriodIdentifier, callback
    ) {
        // make (agencyCode,siteNo) digestible by AQUARIUS
        var locationIdentifier = (agencyCode === "USGS") ?
            siteNumber : siteNumber + '-' + agencyCode;

        var obj = {
            token: token,
            format: "json",
            LocationIdentifier: locationIdentifier,
            Parameter: parameter,
            ComputationPeriodIdentifier: computationPeriodIdentifier,
            ExtendedFilters: "[{FilterName:ACTIVE_FLAG,FilterValue:Y}," +
                              "{FilterName:PRIMARY_FLAG,FilterValue:Primary}]"
        };

        if (computationIdentifier)
            obj["ComputationIdentifier"] = computationIdentifier;

        try {
            rest.query(
                hostname,
                "GET",
                undefined,      // HTTP headers
                "/AQUARIUS/Publish/V2/GetTimeSeriesDescriptionList",
                obj,
                false,
                callback
            );
        }
        catch (error) {
            callback(error);
            return;
        }
    } // getTimeSeriesDescriptionList

    /**
       @method
       @description Get a TimeSeriesDescription object from AQUARIUS.
       @param {string} agencyCode USGS agency code.
       @param {string} siteNumber USGS site number.
       @param {string} parameter AQUARIUS parameter.
       @param {string} computationIdentifier AQUARIUS computation identifier.
       @param {string} computationPeriodIdentifier AQUARIUS computation
                       period identifier.
       @param {function} outerCallback Callback function to call when complete.
    */
    this.getTimeSeriesDescription = function (
        agencyCode, siteNumber, parameter, computationIdentifier,
        computationPeriodIdentifier, outerCallback
    ) {
        var locationIdentifier, timeSeriesDescription;

        async.waterfall([
            function (callback) {
                getTimeSeriesDescriptionList(
                    agencyCode, siteNumber, parameter,
                    computationIdentifier,
                    computationPeriodIdentifier, callback
                );
            },
            /**
               @function
               @description Receive response from AQUARIUS
                            GetTimeSeriesDescriptionList, then parse
                            list of related TimeSeriesDescriptions to
                            query AQUARIUS GetTimeSeriesCorrectedData
                            service.
               @callback
               @param {string} messageBody Message body part of HTTP
                               response from
                               GetTimeSeriesDescriptionList.
            */
            function (messageBody, callback) {
                var timeSeriesDescriptionListServiceResponse;

                try {
                    timeSeriesDescriptionListServiceResponse =
                        JSON.parse(messageBody);
                }
                catch (error) {
                    callback(error);
                    return;
                }

                callback(
                    null,
                timeSeriesDescriptionListServiceResponse.TimeSeriesDescriptions
                );
            },
            /**
               @function
               @description Check for zero TimeSeriesDescriptions
                            returned from AQUARIUS Web service query
                            above.
               @callback
            */
            function (timeSeriesDescriptions, callback) {
                locationIdentifier =
                    new LocationIdentifier(agencyCode, siteNumber);

                if (timeSeriesDescriptions.length === 0) {
                    callback(
                        "No time series description list found at " +
                            url.format({
                                protocol: "http",
                                host: hostname,
                                pathname:
                           "/AQUARIUS/Publish/V2/GetTimeSeriesDescriptionList",
                                query: {
                                    token: token,
                                    format: "json",
                                    LocationIdentifier: locationIdentifier,
                                    Parameter: parameter,
                                   ComputationIdentifier: computationIdentifier,
                                    ComputationPeriodIdentifier:
                                       computationPeriodIdentifier,
                                    ExtendedFilters:
                                    "[{FilterName:ACTIVE_FLAG,FilterValue:Y}," +
                                        "{FilterName:PRIMARY_FLAG,FilterValue:Primary}]"
                                }
                            })
                    );
                    return;
                }

                callback(null, timeSeriesDescriptions);
            },
            /**
               @function
               @description For each AQUARIUS time series description,
                            weed out non-primary ones.
               @callback
            */
            function (timeSeriesDescriptions, callback) {
                /**
                   @private
                   @todo Need to decide whether <code>distill()</code>
                         is to be public method or private function.
                */
                timeSeriesDescription = distill(
                    timeSeriesDescriptions, locationIdentifier,
                    callback
                );
            },
            function (tsd, callback) {
                timeSeriesDescription = tsd;
                callback(null);
            }
        ],
        function (error) {
            if (error)
                outerCallback(error);
            else
                outerCallback(null, timeSeriesDescription);
        });
    } // getTimeSeriesDescription

}, // AQUARIUS

NWISRA: function (host, userName, password, log, callback) {
    var authentication;
    var parameters;

    /**
       @method
       @description Get authentication token from NWIS-RA.
       @private
     */
    function authenticate(callback) {
        async.waterfall([
            function (callback) {
                var path = "/service/auth/authenticate";

                /**
                   @description Handle response from HTTPS query.
                   @callback
                */
                function queryCallback(response) {
                    var messageBody = "";

                    // accumulate response
                    response.on(
                        "data",
                        function (chunk) {
                            messageBody += chunk;
                        });

                    response.on("end", function () {
                        if (log)
                            console.log(
                        "service.NWISRA.authenticate.response.statusCode: " +
                                    response.statusCode.toString()
                            );

                        if (response.statusCode < 200 ||
                            300 <= response.statuscode) {
                            callback(
                                "Could not reference site at http://" + host +
                                    path + "; HTTP status code was: " +
                                    response.statusCode.toString()
                            );
                            return;
                        }

                        callback(null, messageBody);
                    });
                }

                var request = https.request({
                    host: host,
                    method: "POST",
                    headers:
                        {"content-type": "application/x-www-form-urlencoded"},
                    path: path
                }, queryCallback);

                /**
                   @description Handle service invocation errors.
                */
                request.on("error", function (error) {
                    if (error.code === "ENOTFOUND") {
                        // can't get to the NWIS-RA server; use local file
                        fs.readFile("parameters.json", function (error, json) {
                            if (error) {
                                callback(error);
                                return;
                            }

                            try {
                                parameters = JSON.parse(json);
                            }
                            catch (error) {
                                callback(error);
                                return;
                            }
                        });
                        callback(null, null);
                    }
                    else
                        callback(error);
                    return;
                });

                request.write(
                    querystring.stringify(
                        {username: userName, password: password}
                    )
                );

                request.end();
            },
            function (messageBody, callback) {
                if (messageBody) {
                    try {
                        authentication = JSON.parse(messageBody);
                    }
                    catch (error) {
                        callback(error);
                        return;
                    }
                }
                callback(null);
            }
        ],
            function (error) {
                if (error)
                    callback(error);
                else
                    callback(null);
            }
        );
    } // authenticate

    /**
       @method
       @description Query an NWIS-RA Web service.
       @private
       @param {string} path Path to Web service endpoint.
       @param {object} obj Web service REST parameters.
       @param {boolean} log Enable logging if true; no logging otherwise.
       @callback {function} Callback function to call when query complete.
     */
    function queryRemote(path, obj, log, callback) {
        /**
           @description Handle response from HTTPS query.
           @callback
        */
        function queryCallback(response) {
            var messageBody = '';

            // accumulate response
            response.on(
                'data',
                function (chunk) {
                    messageBody += chunk;
                });

            response.on('end', function () {
                if (log)
                    console.log("rest.queryRemote.response.statusCode: " +
                                response.statusCode.toString());

                if (response.statusCode === 404) {
                    callback("Site not found at http://" + host);
                    return;
                }
                else if (
                    response.statusCode < 200 || 300 <= response.statuscode
                ) {
                    callback(
                        "Could not reference site at http://" + host +
                            path + "; HTTP status code was: " +
                            response.statusCode.toString()
                    );
                    return;
                }
                callback(null, messageBody);
                return;
            });
        } // queryCallback

        var request = https.request({
            host: host,
            method: "GET",
            headers: {"Authorization": "Bearer " + authentication.tokenId},
            path: path + querystring.stringify(obj)
        }, queryCallback);

        /**
           @description Handle service invocation errors.
        */
        request.on("error", function (error) {
            if (log)
                console.log("rest.queryRemote: " + error);
            callback(error);
            return;
        });

        request.end();

    } // queryRemote

    /**
       @method
       @description Make an NWIS-RA, HTTP query.
       @public
       @param {object} obj HTTP query parameter/value object.
       @param {boolean} log Enable console logging if true; no console
                        logging when false.
       @param {function} callback Callback function to call if/when
                         response is received.
    */
    this.query = function (obj, log, callback) {
        // if NWIS-RA is logged-into
        if (authentication)
            queryRemote(
                "/service/data/view/parameters/json",
                obj,
                log,
                callback
            );
        else
            // emulate the query by referring to local copy of JSON data
            callback(
                parameters.find(function (record) {
                    if (obj["parameters.PARM_CD"] === record.PARM_CD)
                        return true;
                    else
                        return false;
                })
            );
    } // query

    // constructor
    var host = host;
    var log = log;

    authenticate(callback);

} // NWISRA

}
