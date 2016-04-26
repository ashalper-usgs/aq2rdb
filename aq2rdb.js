/**
 * @fileOverview A Web service to map AQUARIUS, time series data
 *               requests to USGS-variant RDB files.
 *
 * @module aq2rdb
 *
 * @author Andrew Halper <ashalper@usgs.gov>
 *
 * @see <a href="https://sites.google.com/a/usgs.gov/nwis_integrator/data_retrieval/cli/aqts2rdb">aqts2rdb</a>.
 */

'use strict';

// Node.js modules
var async = require('async');
var commandLineArgs = require('command-line-args');
var ifAsync = require('if-async');
var fs = require('fs');
var http = require('http');
var httpdispatcher = require('httpdispatcher');
var moment = require('moment-timezone');
var path = require('path');
var querystring = require('querystring');
var sprintf = require("sprintf-js").sprintf;
var url = require('url');

// aq2rdb modules
var adaps = require('./adaps');
var rdb = require('./rdb');
var rest = require('./rest');
var site = require('./site');

/**
   @description The Web service name is the script name without the
                ".js" suffix.
   @global
   @type {string}
   @constant
*/
var packageName = path.basename(process.argv[1]).slice(0, -3);

/**
   @description The domain of supported, server-side, aq2rdb command
                line arguments.
   @global
   @private
   @type {object}
   @see https://www.npmjs.com/package/command-line-args#synopsis
*/
var cli = commandLineArgs([
    /** @description Print version and exit. */
    {name: "version", alias: 'v', type: Boolean, defaultValue: false},
    /** @description Enable logging. */
    {name: "log", alias: 'l', type: Boolean, defaultValue: false},
    /** @description TCP/IP port that aq2rdb will listen on. */
    {name: "port", alias: 'p', type: Number, defaultValue: 8081},
    /** @description DNS name of AQUARIUS Web service host. */
    {name: "aquariusHostname", alias: 'a', type: String,
     defaultValue: "nwists.usgs.gov"},
    /**
       @description AQUARIUS Web service host, service account user
                    name.
    */
    {name: "aquariusUserName", type: String},
    /**
       @description AQUARIUS Web service host, service account
                    password.
    */
    {name: "aquariusPassword", type: String},
    /** @description DNS name of aquarius-token Web service host. */
    {name: "aquariusTokenHostname", alias: 't', type: String,
     defaultValue: "localhost"},
    /** @description DNS name of USGS Water Services Web service host. */
    {name: "waterServicesHostname", type: String,
     defaultValue: "waterservices.usgs.gov"},
    /**
       @description DNS name of USGS NWIS Reporting Application
                    service host.
    */
    {name: "waterDataHostname", type: String,
     defaultValue: "nwisdata.usgs.gov"},
    /**
       @description USGS NWIS Reporting Application, service account
                    user name.
    */
    {name: "waterDataUserName", type: String},
    /**
       @description USGS NWIS Reporting Application, service account
                    password.
    */
    {name: "waterDataPassword", type: String}
]);

/**
   @description AQUARIUS, Web service object.
   @global
   @private
   @type {object}
*/
var aquarius;

/**
   @description NWIS-RA, Web service object.
   @global
   @private
   @type {object}
*/
var nwisRA;

/**
   @description NWIS STAT, domain table object.
   @global
   @private
   @type {object}
*/
var stat;

/**
   @description Parsed, server-side, aq2rdb command line arguments.
   @global
   @private
   @type {object}
   @see https://www.npmjs.com/package/command-line-args#module_command-line-args--CommandLineArgs+parse
*/
var options;

/**
   @description A mapping of select NWIS time zone codes to IANA time
                zone names (referenced by moment-timezone
                module). This is not a complete enumeration of the
                time zones defined in the NWIS TZ table, but the time
                zone abbreviations known (presently) to be related to
                all SITEFILE sites in NATDB.
   @global
   @private
   @type {object}
   @constant
*/
var tzName = {
    /**
       @property DST is not observed in Afghanistan.
       @see https://en.wikipedia.org/wiki/Time_in_Afghanistan
    */
    AFT:   {N: "Asia/Kabul", Y: "Asia/Kabul"},
    AKST:  {N: "Etc/GMT-9",  Y: "America/Anchorage"},
    AST:   {N: "Etc/GMT-4",  Y: "America/Glace_Bay"},
    AWST:  {N: "Etc/GMT+4",  Y: "Australia/Perth"},
    BT:    {N: "Etc/GMT+3",  Y: "Asia/Baghdad"},
    CST:   {N: "Etc/GMT-6",  Y: "America/Chicago"},
    DST:   {N: "Etc/GMT+1",  Y: "Etc/GMT+1"},
    EET:   {N: "Etc/GMT+2",  Y: "Europe/Athens"},
    EST:   {N: "Etc/GMT-5",  Y: "America/New_York"},
    GMT:   {N: "Etc/GMT+0",  Y: "Europe/London"},
    GST:   {N: "Etc/GMT+10", Y: "Pacific/Guam"},
    HST:   {N: "Etc/GMT-10", Y: "HST"},
    /**
       @property IANA time zone mapping of NWIS's "International Date
                 Line, East".
    */
    IDLE:  {N: "Etc/GMT+12", Y: "Etc/GMT+12"},
    /**
       @property IANA time zone mapping of NWIS's "International Date
                 Line, West".
    */
    IDLW:  {N: "Etc/GMT-12", Y: "Etc/GMT-12"},
    JST:   {N: "Etc/GMT+9",  Y: "Asia/Tokyo"},
    MST:   {N: "America/Phoenix",  Y: "America/Denver"},
    /**
       @property IANA time zone mapping of NWIS's "Newfoundland
                 Standard Time, local time not acknowledged".
       @description moment-timezone has no support for UTC-03:30 [in
                    the context of Northern Hemisphere summer], which
                    would be the correct mapping of NWIS's (NST,N)
                    [i.e., "Newfoundland Standard Time, local time not
                    acknowledged"] SITEFILE predicate.
    */
    NST:   {N: "UTC-03:30",  Y: "America/St_Johns"},
    NZT:   {N: "Etc/GMT+12", Y: "NZ"},
    PST:   {N: "Etc/GMT-8",  Y: "America/Los_Angeles"},
    /**
       @property IANA time zone mapping for NWIS's "South Australian Standard
                 Time, local time not acknowledged"
       @description moment-timezone has no support for UTC+09:30 [in
                    the context of Southern Hemisphere summer], which
                    would be the mapping of NWIS" (SAT,N) [i.e.,
                    "South Australian Standard Time, local time not
                    acknowledged"].
    */
    SAT:   {N: "UTC+09:30",  Y: "Australia/Adelaide"},
    UTC:   {N: "Etc/GMT+0",  Y: "Etc/GMT+0"},
    WAST:  {N: "Etc/GMT+7",  Y: "Australia/Perth"},
    WAT:   {N: "Etc/GMT+1",  Y: "Africa/Bangui"},
    ZP11:  {N: "Etc/GMT+11", Y: "Etc/GMT+11"},
    ZP4:   {N: "Etc/GMT+4",  Y: "Etc/GMT+4"},
    ZP5:   {N: "Etc/GMT+5",  Y: "Etc/GMT+5"},
    ZP6:   {N: "Etc/GMT+6",  Y: "Etc/GMT+6"}
};

// Somewhat surprisingly, the syntax below makes it past the Node.js
// parser, but ends in "SyntaxError: Unexpected token -" when included
// in the object initializer above.
tzName["ZP-11"] = {N: "Etc/GMT-11", Y: "Etc/GMT-11"};

/**
   @description Exports public functions to external dependent modules.
   @global
   @private
   @type {object}
*/
var aq2rdb = module.exports = {
    /**
       @function
       @description Convert AQUARIUS
                    <code>TimeSeriesPoint.Timestamp</code> string to a
                    common NWIS date format.
       @public
       @param {string} timestamp AQUARIUS Timestamp string to convert.
    */
    toNWISDateFormat: function (timestamp) {
        var date = new Date(timestamp);

        return timestamp.split('T')[0].replace(/-/g, '');
    },

    /**
       @function
       @description Convert AQUARIUS
                    <code>TimeSeriesPoint.Timestamp</code> string to a
                    common NWIS time format.
       @public
       @param {string} timestamp AQUARIUS Timestamp string to convert.
    */
    toNWISTimeFormat: function (timestamp) {
        var date = new Date(timestamp);

        return timestamp.split(/[T.]/)[1].replace(/:/g, '');
    },

    /**
       @function
       @description Convert AQUARIUS
                    <code>TimeSeriesPoint.Timestamp</code> string to a
                    common NWIS datetime format.
       @public
       @param {string} timestamp AQUARIUS Timestamp string to convert.
    */
    toNWISDatetimeFormat: function (timestamp) {
        return aq2rdb.toNWISDateFormat(timestamp) +
            aq2rdb.toNWISTimeFormat(timestamp);
    }
}; // public functions

/**
   @function
   @description This module's logging function, mostly for convenience
                purposes.
   @private
   @param {string} prefix Prefix to prepend to log message.
   @param {string} message Log message.
*/ 
function log(prefix, message) {
    if (options.log)
        console.log(prefix + ": " + message);
} // log

/**
   @function
   @description Error handler.
   @private
   @param {object} error "Error" object.
*/ 
function handle(error) {
    var statusMessage, statusCode;

    /**
       @see https://nodejs.org/api/errors.html#errors_error_code
    */
    if (error.code === 'ECONNREFUSED') {
        statusMessage = '# ' + packageName +
            ': Connection error; a common cause of this ' +
            'is GetAQToken being unreachable';
        /**
           @description "Bad Gateway"
           @private
           @see http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html
        */
        statusCode = 502;
    }
    else if (error instanceof ReferenceError) {
        statusMessage =
            '# ' + packageName + ': There is an undefined ' +
            'reference on the ' + packageName + ' server';
        statusCode = 500;

        log(packageName + ".handle()",
            error.toString().replace(/: (\w+)/, ': "$1"'));
    }
    else if (typeof error === 'string') {
        statusMessage = '# ' + packageName + ': ' + error;
        /**
           @default HTTP error status code.
           @private
           @todo It would be nice to refine this. Too generic now.
        */
        statusCode = 404;
    }
    else {
        statusMessage = '# ' + packageName + ': ' + error.message;
        /**
           @default HTTP error status code.
           @private
           @todo It would be nice to refine this. Too generic now.
        */
        statusCode = 404;
    }

    return [statusCode, statusMessage];
} // handle

/**
   @function
   @description Error messager for JSON parse errors.
   @private
   @param {object} response IncomingMessage object created by Node.js
                   http.Server.
   @param {string} message Error message to display in an RDB comment.
*/
function jsonParseErrorMessage(response, message) {
    var statusMessage = 'While trying to parse a JSON response ' +
        'from AQUARIUS: ' + message;

    response.writeHead(502, statusMessage,
                       {'Content-Length': statusMessage.length,
                        'Content-Type': 'text/plain'});
    response.write(statusMessage, 'ascii');
}

/**
   @class
   @classdesc LocationIdentifier object prototype.
   @private
   @param {string} text AQUARIUS LocationIdentifier string.
*/
var LocationIdentifier = function (text) {
    var text = text;

    /**
       @method
       @description Agency code accessor method.
    */
    this.agencyCode = function () {
        // if agency code delimiter ("-") is present in location
        // identifier
        if (text.search('-') === -1) {
            /**
               @default Agency code.
            */
            return 'USGS';
        }
        else {
            // parse (agency code, site number) embedded in
            // locationIdentifier
            var s = text.split('-');
            return s[1];
        }
    }

    /**
       @method
       @description Site number accessor method.
    */
    this.siteNumber = function () {
        // if agency code delimiter ("-") is present in location
        // identifier
        if (text.search('-') === -1) {
            return text;
        }
        else {
            // parse (agency code, site number) embedded in
            // locationIdentifier
            var s = text.split('-');
            return s[0];
        }
    }

    /**
       @method
       @description Return a string representation of
                    LocationIdentifier.
    */
    this.toString = function () {
        return text;
    }

} // LocationIdentifier

/**
   @function
   @description Check for documentation request, and serve
                documentation if appropriate.
   @private
   @param {string} url Endpoint URL.
   @param {string} name Endpoint name.
   @param {object} response IncomingMessage object created by Node.js
                   http.Server.
   @param {function} callback Callback function to call when complete.
*/
function docRequest(url, servicePath, response, callback) {
    // if this is a documentation request
    if (url === servicePath) {
        // read and serve the documentation page
        fs.readFile(
            "doc/" + path.basename(servicePath) + ".html",
            function (error, html) {
                if (error) {
                    callback(error);
                    return true;
                }
                response.writeHeader(200, {"Content-Type": "text/html"});  
                response.end(html);
            }
        );
        return true;
    }
    else
        return false;
} // docRequest

/**
   @function
   @description Create RDB, DV table row.
   @private
   @param {string} timestamp AQUARIUS timestamp string.
   @param {object} value Time series daily value.
   @param {object} qualifiers AQUARIUS
          QualifierListServiceResponse.Qualifiers.
   @param {object} remarkCodes An array (as domain table) of daily
          values remark codes, indexed by AQUARIUS
          QualifierMetadata.Identifier.
   @param {string} qa QA code.
*/
function dvTableRow(timestamp, value, qualifiers, remarkCodes, qa) {
    // TIME column is always empty for daily values
    var row = moment(timestamp).format("YYYYMMDD") + "\t\t" +
        value.Display + '\t';

    /**
       @author Scott Bartholoma <sbarthol@usgs.gov>
       @since 2015-09-29T10:57-07:00

       @description Remark will have to be derived from the Qualifier
                    section of the response. It will have begin and
                    end dates for various qualification periods.
    */
    async.detect(qualifiers, function (qualifier, callback) {
        var pointTime, startTime, endTime;

        try {
            pointTime = new Date(timestamp);
        }
        catch (error) {
            throw error;
            return;
        }

        try {
            startTime = new Date(qualifier.StartTime);
        }
        catch (error) {
            throw error;
            return;
        }

        try {
            endTime = new Date(qualifier.EndTime);
        }
        catch (error) {
            throw error;
            return;
        }

        // if this daily value's date point intersects the qualifier
        // interval
        if (startTime <= pointTime && pointTime <= endTime) {
            if (remarkCodes[qualifier.Identifier] !== undefined) {
                row += remarkCodes[qualifier.Identifier].toLowerCase();
            }
            else {
                throw 'No remark code found for "' +
                    qualifier.Identifier + '"';
                return;
            }
            callback(true);
        }
    }, function (result) {
        row += "\t ";
    });

    /**
       @author Scott Bartholoma <sbarthol@usgs.gov>
       @since 2015-09-29T10:57-07:00
       @private
       @description I think some of what used to be flags are now
                    Qualifiers. Things like thereshold [sic]
                    exceedances [sic] (high, very high, low, very low,
                    rapid increace/decreast [sic], etc.). The users
                    might want you to put something in that column for
                    the Method and Grade sections of the response as
                    well
    */
    row += '\t' +

    /**
       @author Scott Bartholoma <sbarthol@usgs.gov>
       @since 2015-09-29T10:57-07:00
       @private
       @description Type I would put in something like "R" for raw and
                    "C" for corrected depending on which get method
                    was used. That is similar to what C (computed) and
                    E (Edited) meant for DV data in Adaps.  We don't
                    explicitly have the Meas, Edit, and Comp UV types
                    anymore, they are separate timeseries in AQUARIUS.
    */
    "\tC\t" + qa + '\n';

    return row;
} // dvTableRow

/**
   @function
   @description Patch up some obscure incompatibilities between NWIS's
                site time offset predicate and IANA time zone data
                (used by moment-timezone).
   @private
*/
function nwisVersusIANA(timestamp, name, tzCode, localTimeFlag) {
    var m = moment.tz(timestamp, name);
    var p = new Object();       // datetime point

    // if this site's time offset predicate is "local time not
    // acknowledged", and observes Newfoundland Standard Time or South
    // Australian Standard Time, and date point is within the
    // associated, effective daylight saving time interval
    if (localTimeFlag === 'N' &&
        (tzCode === 'NST' && m.zoneAbbr() === 'NDT' ||
         tzCode === 'SAT' && m.zoneAbbr() === 'ACDT')) {
        var t = new Date(point.Timestamp);
        // normalize time to UTC, then apply time zone offset from UTC
        var offset = name.replace('UTC', '');
        var invertedOffset =
            offset.replace('+', '-') || offset.replace('-', '+');
        var utc = new Date(t.toISOString().replace('Z', invertedOffset));
        var v = utc.toISOString().split(/[T.]/);

        // reformat ISO 8601 date/time to NWIS date/time format
        p.date = v[0].replace('-', '');
        p.time = v[1].replace(':', '');
        // use (non-IANA) name as time zone abbreviation
        p.tz = name;
    }
    else {
        // use IANA time zone data
        p.date = m.format('YYYYMMDD');
        p.time = m.format('hhmmss');
        p.tz = m.format('z');
    }

    return p;
} // nwisVersusIANA

/**
   @class
   @classdesc AQUARIUS object prototype.
   @private
   @param {string} hostname DNS host name of AQUARIUS server.
   @param {string} userName AQUARIUS account user name.
   @param {string} password AQUARIUS account password.
   @param {function} callback Callback to call when object
          constructed.
*/
var AQUARIUS = function (hostname, userName, password, callback) {
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

    var token;

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

    var port = '8080';
    var path = '/services/GetAQToken?';
    var uriString = 'http://' + hostname + '/AQUARIUS/';

    log(packageName + ".AQUARIUS()",
        "querying http://" + hostname + ":" + port +
        path + "..., AQUARIUS server at " + uriString);

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
        host: options.aquariusTokenHostname,
        port: port,             // TODO: make a CLI parameter?
        path: path
    }, getAQTokenCallback);

    /**
       @description Handle GetAQToken service invocation errors.
       @callback
    */
    request.on('error', function (error) {
        var statusMessage;

        log(packageName + ".AQUARIUS().request.on()", error);
        
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
    this.getTimeSeriesCorrectedData = function (
        parameters, callback
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
    this.parseTimeSeriesDataServiceResponse = function (messageBody, callback) {
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
                options.log,
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
                    new LocationIdentifier(siteNumber + '-' + agencyCode);

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

} // AQUARIUS

function required(options, propertyList) {
    for (var i in propertyList){
        if (options[propertyList[i]] === undefined) {
            console.log(
                packageName +
                    ": required command-line argument " + "\"" +
                    propertyList[i] + "\" not found"
            );
            process.exit(1);
        }
    }
} // checkRequiredOption

/**
   @function
   @description Parse aq2rdb?t=UV endpoint's fields. Proceed to next
                async.waterfall() function if successful.
   @private
   @callback
   @param {object} requestURL request.url object to parse.
   @param {function} node-async callback.
*/
function parseFields(requestURL, callback) {
    var field;

    try {
        field = url.parse(requestURL, true).query;
    }
    catch (error) {
        callback(error);
        return;
    }

    // if any mandatory fields are missing
    if (field.t === undefined || field.n === undefined ||
        field.b === undefined || field.e === undefined ||
        field.s === undefined || field.p === undefined) {
        // terminate response with an error
        callback(
            "All of \"t\", \"n\", \"b\", \"e\", \"s\" and \"p\" " +
                "fields must be present" 
        );
        return;
    }

    for (var name in field) {
        if (name.match(/^(a|p|t|s|n|b|e|l|r|w)$/)) {
            // aq2rdb fields
        }
        else {
            callback('Unknown field "' + name + '"');
            return;
        }
    }

    // default "rounding suppression flag"
    var rndsup = (field.r === undefined) ? false : field.r;

    // pass parsed field values to next async.waterfall() function
    callback(
        null, field.t, rndsup, field.w, false, false, field.a, field.n,
        field.p, field.s, field.b, field.e, field.l, ""
    );
} // parseFields

/**
   @description Parse version number from "package.json" and pass to a
                callback function.
   @private
   @param {function} Callback function to call if successful.
 */
function getVersion(callback) {
    fs.readFile("package.json", function (error, json) {
        if (error) {
            callback(error);
            return;
        }
        
        var pkg;
        try {
            pkg = JSON.parse(json);
        }
        catch (error) {
            return;
        }

        callback(pkg.version);  // pass version number to callback function
    });
} // getVersion

/**
   @function
   @description Convert NWIS datetime format to ISO format for
                digestion by AQUARIUS REST query. Offset times from
                time zone of site to UTC to get correct results.
   @private
   @param {object} parameters HTTP query parameters to append to REST
          query search condition.
   @param {object} during Interval object.
   @param {string} tzCode Time zone abbreviation.
   @param {string} fromTheBeginningOfTimeToken Token used in NWIS
          ADAPS to represent the "from the beginning of time" search
          condition.
   @param {string} toTheEndOfTimeToken Token used in NWIS ADAPS to
          represent the "to the end of time" search condition.
   @param {function} callback Callback function to call when complete.
   @see http://momentjs.com/timezone/docs/#/using-timezones/
*/
function appendIntervalSearchCondition(
    parameters, during, tzCode,
    fromTheBeginningOfTimeToken, toTheEndOfTimeToken,
    callback
) {
    // if "from" interval boundary is not "from the beginning of time"
    if (during.from !== fromTheBeginningOfTimeToken) {
        var queryFrom;

        try {
            queryFrom = moment.tz(
                moment(during.from, "YYYYMMDDHHmmss").format(),
                tzCode
            ).format();
        }
        catch (error) {
            log(packageName + ".error", error);
            callback(error);
            return;
        }
        parameters["QueryFrom"] = queryFrom;
    }

    // if "to" interval boundary is not "to the end of time"
    if (during.to !== toTheEndOfTimeToken) {
        var queryTo;

        try {
            queryTo = moment.tz(
                moment(during.to, "YYYYMMDDHHmmss").format(),
                tzCode
            ).format();
        }
        catch (error) {
            log(packageName + ".error", error);
            callback(error);
            return;
        }
        parameters["QueryTo"] = queryTo;
    }

    return parameters;
} // appendIntervalSearchCondition

/**
   @function
   @description Produce daily values table body.
   @private
   @param {string} timeSeriesUniqueId AQUARIUS time series unique ID
          key.
   @param {string} queryFrom Start date of interval predicate.
   @param {string} queryTo End date of interval predicate.
   @param {string} tzName moment.tz time zone name.
   @param {object} response HTTP response object to send table body to.
   @param {function} callback Callback function to call when complete.
*/
function dvTableBody(
    timeSeriesUniqueId, queryFrom, queryTo, tzName, response, callback
) {
    var remarkCodes;

    async.waterfall([
        /**
           @function
           @description Request remark codes from AQUARIUS.
           @callback
           @todo This is fairly kludgey, because remark codes might
                 not be required for every DV interval; try to nest in
                 a conditional eventually.
        */
        function (callback) {
            try {
                rest.query(
                    aquarius.hostname,
                    "GET",
                    undefined,      // HTTP headers
                    "/AQUARIUS/Publish/V2/GetQualifierList/",
                    {token: aquarius.token(), format: "json"},
                    options.log,
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
                        aquarius.hostname +
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

            // proceed to next waterfall
            callback(null);
        },
        /**
           @function
           @description Query AQUARIUS GetTimeSeriesCorrectedData to
                        get related daily values.
           @callback
        */
        function (callback) {
            var f, t;
            var parameters = {
                TimeSeriesUniqueId: timeSeriesUniqueId,
                ApplyRounding: "true"
            };

            if (queryFrom !== "00000000") {
                try {
                    f = moment.tz(queryFrom, tzName).format("YYYY-MM-DD");
                }
                catch (error) {
                    callback(error);
                    return;
                }
                parameters["QueryFrom"] = f;
            }

            if (queryTo !== "99999999") {
                try {
                    t = moment.tz(queryTo, tzName).format("YYYY-MM-DD");
                }
                catch (error) {
                    callback(error);
                    return;
                }
                parameters["QueryTo"] = t;
            }

            try {
                aquarius.getTimeSeriesCorrectedData(
                    parameters, callback
                );
            }
            catch (error) {
                callback(error);
                return;
            }
        },
        aquarius.parseTimeSeriesDataServiceResponse,
        /**
           @function
           @description Write each RDB row to HTTP response.
           @callback
        */
        function (timeSeriesDataServiceResponse, callback) {
            async.each(
                timeSeriesDataServiceResponse.Points,
                /**
                   @description Write an RDB row for one time series
                   point.
                   @callback
                */
                function (timeSeriesPoint, callback) {
                    response.write(
                        dvTableRow(
                            timeSeriesPoint.Timestamp,
                            timeSeriesPoint.Value,
                            timeSeriesDataServiceResponse.Qualifiers,
                            remarkCodes,
          timeSeriesDataServiceResponse.Approvals[0].LevelDescription.charAt(0)
                        ),
                        "ascii"
                    );
                    callback(null);
                }
            );
            callback(null);
        }
    ],
        function (error) {
            if (error) {
                callback(error);
                return;
            }
            else {
                callback(null);
            }
        }
    ); // async.waterfall
} // dvTableBody

/**
   @description GetDVTable endpoint service request handler.
*/
httpdispatcher.onGet(
    '/' + packageName + '/GetDVTable',
    /**
       @callback
    */
    function (request, response) {
        var field, token, locationIdentifier, timeSeriesDescription;

        /**
           @see https://github.com/caolan/async
        */
        async.waterfall([
            /**
               @function
               @description Check for documentation request.
               @callback
            */
            function (callback) {
                if (docRequest(request.url, '/aq2rdb/GetDVTable',
                               response, callback))
                    return;
                callback(null);
            },
            /**
               @function
               @description Parse fields and values in GetDVTable URL.
               @callback
            */
            function (callback) {
                try {
                    field = url.parse(request.url, true).query;
                }
                catch (error) {
                    callback(error);
                    return;
                }

                for (var name in field) {
                    if (name === 'LocationIdentifier') {
                        locationIdentifier =
                            new LocationIdentifier(field.LocationIdentifier);
                    }
                    else if (name.match(
                        /^(Parameter|ComputationIdentifier|QueryFrom|QueryTo)$/
                    )) {
                        // AQUARIUS fields
                    }
                    else {
                        callback('Unknown field "' + name + '"');
                        return;
                    }
                }

                if (locationIdentifier === undefined) {
                    callback('Required field "LocationIdentifier" not found');
                    return;
                }

                if (field.Parameter === undefined) {
                    callback('Required field "Parameter" not found');
                    return;
                }

                callback(
                    null, locationIdentifier.agencyCode(),
                    locationIdentifier.siteNumber(), field.Parameter,
                    undefined, "Daily"
                );
            },
            aquarius.getTimeSeriesDescription,
            function (tsd, callback) {
                timeSeriesDescription = tsd;

                callback(
                    null, options.waterServicesHostname,
                    locationIdentifier.agencyCode(),
                    locationIdentifier.siteNumber(), options.log
                );
            },
            site.request,
            site.receive,
            /**
               @function
               @description Write RDB header and heading.
               @callback
            */
            function (site, callback) {
                async.series([
                    /**
                       @function
                       @description Write HTTP response header.
                       @callback
                    */
                    function (callback) {
                        response.writeHead(
                            200, {"Content-Type": "text/plain"}
                        );
                        callback(null);
                    },
                    /**
                       @function
                       @description Write RDB header to HTTP response.
                       @callback
                    */
                    function (callback) {
                        rdb.header(
                            "NWIS-I DAILY-VALUES", "YES", site,
                            subLocationIdentifer, parameter,
                            /**
                               @todo Statistic code is locked up
                                     within the scope of parseFields()
                                     right now. Need to free it, and
                                     look up (name,description) as
                                     well if possible.
                            */
                            {code: "", name: "", description: ""},
                            range,
                            callback
                        );
                    },
                    /**
                       @function
                       @description Write RDB body to HTTP response.
                       @callback
                    */
                    function (callback) {
                        var start, end;

                        if (field.QueryFrom !== undefined) {
                            start = aq2rdb.toNWISDateFormat(field.QueryFrom);
                        }

                        if (field.QueryTo !== undefined) {
                            end = aq2rdb.toNWISDateFormat(field.QueryTo);
                        }

                        var header = aq2rdb.rdbHeaderBody(
                            'NWIS-I DAILY-VALUES', site,
                            timeSeriesDescription.SubLocationIdentifer,
                            {start: start, end: end}
                        );
                        response.write(header, 'ascii');
                        callback(null);
                    },
                    /**
                       @function
                       @description Write RDB heading (a different
                                    thing than RDB header, above) to
                                    HTTP response.
                       @callback
                    */
                    function (callback) {
                        response.write(
                            'DATE\tTIME\tVALUE\tREMARK\tFLAGS\tTYPE\tQA\n' +
                                '8D\t6S\t16N\t1S\t32S\t1S\t1S\n', 'ascii'
                        );

                        callback(
                            null, timeSeriesDescription.UniqueId,
                            field.QueryFrom, field.QueryTo,
             tzName[waterServicesSite.tzCode][waterServicesSite.localTimeFlag],
                            response
                        );
                    },
                    dvTableBody
                ]);
                callback(null);
            }
        ],
        /**
           @description node-async error handler function for
                        outer-most, GetDVTable async.waterfall
                        function.
           @callback
        */
        function (error) {
            if (error) {
                handle(error, response);
            }
            response.end();
        }
      );
    }
); // GetDVTable

/**
   @description GetUVTable endpoint service request handler.
*/
httpdispatcher.onGet(
    '/' + packageName + "/GetUVTable",
    /**
       @callback
    */
    function (request, response) {
        var field, token, locationIdentifier, site, parameter;

        async.waterfall([
            function (callback) {
                if (docRequest(request.url, "/aq2rdb/GetUVTable",
                               response, callback))
                    return;
                callback(null);
            },
            function (callback) {
                /**
                   @todo See if code can be factored-out of "/aq2rdb"
                         endpoint to (re-)implement GetUVTable.
                */
                callback(null);
            }
        ],
            /**
               @description node-async error handler function for
                            outer-most, GetUVTable async.waterfall
                            function.
               @callback
            */
            function (error) {
                if (error) {
                    handle(error, response);
                }
                response.end();
            }
        );
    }
); // GetUVTable

/**
   @description aq2rdb endpoint, service request handler.
*/
httpdispatcher.onGet(
    '/' + packageName,
    /**
       @callback
    */
    function (request, response) {
        var dataType, agencyCode, siteNumber, locationIdentifier;
        var waterServicesSite;
        /**
           @todo Need to check downstream depedendencies in aq2rdb
                 endpoint's async.waterfall function for presence of
                 former "P" prefix of this value.
        */
        var parameterCode;
        var parameter, statCode, extendedFilters;
        var during, editable, cflag, vflag;
        var rndsup, locTzCd;

        /**
           @function
           @description node-if-async predicate function, called by
                        ifAsync() in async.waterfall() below.
           @see https://github.com/ironSource/node-if-async
        */
        function dataTypeIsDV(callback) {
            if (dataType === "DV")
                callback(null, true);
            else
                callback(null, false);
        }

        /**
           @function
           @description node-if-async predicate function, called by
                        ifAsync() in async.waterfall() below.
           @see https://github.com/ironSource/node-if-async
        */
        function dataTypeIsUV(callback) {
            if (dataType === "UV")
                callback(null, true);
            else
                callback(null, false);
        }

        /**
           @function
           @description A Node.js emulation of legacy NWIS,
                        FDVRDBOUT() Fortran subroutine: "Write DV data
                        in rdb FORMAT" [sic].
        */
        function dailyValues(callback) {
            var parameters = Object();
            // many/most of these are artifacts of the legacy code,
            // and probably won't be needed:
            var rndary = ' ';
            var rndparm, rnddd;
            var pcode, timeSeriesDescription;

            async.waterfall([
                function (callback) {
                    callback(
                        null, options.waterServicesHostname,
                        agencyCode, siteNumber, log
                    );
                },
                site.request,
                site.receive,
                function (receivedSite, callback) {
                    waterServicesSite = receivedSite; // set global
                    callback(null);
                },
                function (callback) {
                    pcode = 'P';         // pmcode            // set rounding
                    /**
                       @todo Load data descriptor?
                       s_mddd(nw_read, irc, *998);
                    */
                    /**
                       @todo call parameter Web service here
                       rtcode = pmretr(60);
                    */
                    if (rnddd !== ' ' && rnddd !== '0000000000')
                        rndary = rnddd;
                    else
                        rndary = rndparm;

                    callback(
                        null, agencyCode, siteNumber,
                        parameter.aquariusParameter, undefined,
                        "Daily"
                    );
                },
                aquarius.getTimeSeriesDescription,
                function (tsd, callback) {                  
                    // save TimeSeriesDescription in outer scope
                    timeSeriesDescription = tsd;

                    var statistic = {code: statCode};

                    try {
                        statistic.name = stat[statCode].name;
                        statistic.description = stat[statCode].description;
                    }
                    catch (error) {
                        callback(
                            "Invalid statistic code \"" + statCode +
                                "\""
                        );
                        return;
                    }

                    // write the header records
                    rdb.header(
                        "NWIS-I DAILY-VALUES",
                        (editable) ? "YES" : "NO",
                        waterServicesSite,
                        timeSeriesDescription.SubLocationIdentifer,
                        parameter, statistic,
                        /**
                           @todo Hard-coded object here is likely not
                                 correct under all circumstances.
                        */
                        {name: "FINAL",
                         description: "EDITED AND COMPUTED DAILY VALUES"},
                        {start: during.from, end: during.to},
                        callback
                    );
                },
                function (header, callback) {
                    // write RDB column headings
                    response.write(
                        header +
                    "DATE\tTIME\tVALUE\tPRECISION\tREMARK\tFLAGS\tTYPE\tQA\n" +
                            "8D\t6S\t16N\t1S\t1S\t32S\t1S\t1S\n",
                        "ascii"
                    );

                    callback(
                        null, timeSeriesDescription.UniqueId,
                        during.from, during.to,
             tzName[waterServicesSite.tzCode][waterServicesSite.localTimeFlag],
                        response
                    );
                },
                dvTableBody
            ],
                function (error) {
                    if (error) {
                        callback(error);
                        return;
                    }
                    else {
                        callback(null);
                    }
                }
            ); // async.waterfall
        } // dailyValues

        function unitValues(callback) {
            var timeSeriesDescription;

            async.waterfall([
                function (callback) {
                    if (agencyCode === undefined) {
                        callback("Required field \"agencyCode\" not found");
                        return;
                    }

                    if (siteNumber === undefined) {
                        callback("Required field \"siteNumber\" not found");
                        return;
                    }

                    // TODO: "parameter" somehow went MIA in unitValues() formal
                    // parameters in translation from Fortran
                    if (parameterCode === undefined) {
                        callback(
                            "Required AQUARIUS field \"Parameter\" not found"
                        );
                        return;
                    }

                    callback(
                        null, options.waterServicesHostname, agencyCode,
                        siteNumber, options.log
                    );
                },
                /**
                   @todo site.request() and site.receive() can be done
                         in parallel with the requesting/receiving of
                         time series descriptions below.
                */
                site.request,
                site.receive,
                function (receivedSite, callback) {
                    waterServicesSite = receivedSite; // set global
                    callback(null);
                },
                function (callback) {
                    callback(
                        null, agencyCode, siteNumber,
                        parameter.aquariusParameter, "Instantaneous",
                        "Points"
                    );
                },
                aquarius.getTimeSeriesDescription,
                function (tsd, callback) {
                    timeSeriesDescription = tsd; // set variable in outer scope
                    callback(null);
                },
                /**
                   @description Write RDB column names and column data
                                type definitions to HTTP response.
                   @callback
                */
                function (callback) {
                    async.waterfall([
                        /**
                           @function
                           @description Write RDB header to HTTP response.
                           @callback
                        */
                        function (callback) {
                            // (indirectly) call rdb.header() (below) with
                            // these arguments
                            callback(
                                null, "NWIS-I UNIT-VALUES",
                                (editable) ? "YES" : "NO",
                                waterServicesSite,
                                timeSeriesDescription.SubLocationIdentifer,
                                /**
                                   @todo need to find out what to pass
                                         in for "statistic" parameter
                                         when doing UVs below.
                                */
                                parameter, undefined,
                                /**
                                   @todo this is pragmatically
                                         hard-coded now, but there is
                                         a relationship to "cflag"
                                         value above.
                                */
                                {code: 'C', name: "COMPUTED"},
                                {start: during.from, end: during.to}
                            );
                        },
                        rdb.header,
                        function (header, callback) {
                            response.write(
                                header +
                "DATE\tTIME\tTZCD\tVALUE\tPRECISION\tREMARK\tFLAGS\tQA\n" +
                                    "8D\t6S\t6S\t16N\t1S\t1S\t32S\t1S\n",
                                "ascii"
                            );
                            callback(null);
                        }
                    ]);
                    callback(null, timeSeriesDescription.UniqueId);
                },
                /**
                   @description Call AQUARIUS GetTimeSeriesCorrectedData
                                service.
                   @callback
                */
                function (uniqueId, callback) {
                    // Note: "rndsup" value is inverted below for semantic
                    // forwards-compatibility with AQUARIUS's
                    // "ApplyRounding" parameter.
                    var parameters = {
                        TimeSeriesUniqueId: uniqueId,
                        ApplyRounding: eval(! rndsup).toString()
                    };

                    parameters = appendIntervalSearchCondition(
                        parameters, during,
                        waterServicesSite.tzCode,
                        "00000000000000", "99999999999999",
                        callback
                    );

                    try {
                        aquarius.getTimeSeriesCorrectedData(
                            parameters, callback
                        );
                    }
                    catch (error) {
                        callback(error);
                        return;
                    }
                },
                /**
                   @description Receive response from AQUARIUS
                                GetTimeSeriesCorrectedData service.
                   @callback
                */
                aquarius.parseTimeSeriesDataServiceResponse,
                /**
                   @description Write time series data as RDB rows to
                                HTTP response.
                   @callback
                */
                function (timeSeriesDataServiceResponse, callback) {
                    async.each(
                        timeSeriesDataServiceResponse.Points,
                        /**
                           @description Write an RDB row for one time
                                        series point.
                           @callback
                        */
                        function (point, callback) {
                            var zone, m;
                            var tzCode = waterServicesSite.tzCode;
                            var localTimeFlag =
                                waterServicesSite.localTimeFlag;

                            try {
                                zone = tzName[tzCode][localTimeFlag];
                            }
                            catch (error) {
                                callback(
                                    "Could not look up IANA time zone " +
                                        "name for site time zone spec. " +
                                        "(" + tzCode + "," + localTimeFlag + ")"
                                );
                                return;
                            }

                            // reference AQUARIUS timestamp to site's
                            // (NWIS) time zone spec.
                            m = moment.tz(point.Timestamp, zone);

                            response.write(
                                m.format("YYYYMMDD") + '\t' +
                                    m.format("HHmmss") + '\t' +
                                    waterServicesSite.tzCode + '\t' +
                                    point.Value.Display + "\t\t \t\t" +
                                    // might not be
                                    // backwards-compatible with
                                    // nwts2rdb:
        timeSeriesDataServiceResponse.Approvals[0].LevelDescription.charAt(0) +
                                    '\n'
                            );
                            callback(null);
                        }
                    );
                    callback(null);
                }
            ],
                function (error) {
                    if (error) {
                        callback(error);
                        return;
                    }
                    else {
                        callback(null);
                    }
                }
            ); // async.waterfall
        } // unitValues

        async.waterfall([
            function (callback) {
                if (docRequest(request.url, "/aq2rdb", response, callback))
                    return;
                callback(null, request.url);
            },
            parseFields,
            function rdbOut(
                dataType, rndsup, wyflag, cflag, vflag, agencyCode,
                siteNumber, parameterCode, instat, begdat, enddat,
                locTzCd, titlline, callback
            ) {
                var datatyp, stat, uvtyp, interval;
                var uvtypPrompted = false;

                if (locTzCd === undefined) locTzCd = "LOC";

                // init control argument
                var sopt = "10000000000000000000000000000000".split("");

                dataType = dataType.substring(0, 2).toUpperCase();

                // convert agency to 5 characters - default to USGS
                if (agencyCode === undefined)
                    agencyCode = "USGS";
                else
                    agencyCode = agencyCode.substring(0, 5);

                // convert station to 15 characters
                siteNumber = siteNumber.substring(0, 15);

                // further processing depends on data type

                if (dataType === 'DV') {
                    if (instat === undefined)
                        sopt[7] = '1';
                    else
                        statCode = instat;
                }

                if (dataType === 'DV' || dataType === 'DC' ||
                    dataType === 'SV' || dataType === 'PK') {
                    // convert dates to 8 characters
                    if (begdat === undefined || enddat === undefined) {
                        if (wyflag)
                            sopt[8] = '4';
                        else
                            sopt[9] = '3';
                    }
                    else {
                        interval =
                            new adaps.IntervalDay(begdat, enddat, wyflag);
                    }
                }

                if (dataType === 'UV') {
                    
                    uvtyp = instat.charAt(0).toUpperCase();
                    
                    if (! (uvtyp === 'C' || uvtyp === 'E' ||
                           uvtyp === 'M' || uvtyp === 'N' ||
                           uvtyp === 'R' || uvtyp === 'S')) {
                        // Position of this is an artifact of the
                        // nwts2rdb legacy code: it might need to be
                        // moved earlier in HTTP query parameter
                        // validation code.
                        callback(
                            'UV type code must be ' +
                                '"M", "N", "E", "R", "S", or "C"'
                        );
                        return;
                    }

                    // convert date/times to 14 characters
                    if (begdat === undefined || enddat === undefined) {
                        if (wyflag)
                            sopt[8] = '4';
                        else
                            sopt[9] = '3';
                    }
                    else {
                        interval =
                            new adaps.IntervalSecond(begdat, enddat, wyflag);
                    }

                }

                // This is where, formerly, NWF_RDB_OUT():
                // 
                //    get PRIMARY DD that goes with parm if parm supplied
                //
                // Since the algorithmic equivalent is now deep within
                // the bowels of unitValues(), it is no longer
                // here. This comment is just a reminder that it used
                // to be here, in case it is later discovered that the
                // primary identification is necessary before
                // unitValues() does it.

                // retrieving measured uvs and transport_cd not
                // supplied, prompt for it
                if (uvtypPrompted && dataType === "UV" &&
                    (uvtyp === 'M' || uvtyp === 'N') &&
                    transport_cd === undefined) {
                    /**
                       @todo Convert to callback error?
                    */
                    /*
                      nw_query_meas_uv_type(
                         agencyCode, siteNumber, ddid, begdtm,
                         enddtm, loc_tz_cd, transport_cd,
                         sensor_type_id, *998)
                      if (transport_cd === undefined) {
                      WRITE (0,2150) agencyCode, siteNumber, ddid
                      2150
                         FORMAT (/,"No MEASURED UV data for station "",A5,A15,
                      "", DD "',A4,'".  Aborting.",/)
                      return irc;
                      END IF
                    */
                }

                callback(
                    null, false, rndsup, cflag, vflag, dataType,
                    agencyCode, siteNumber, parameterCode, interval,
                    locTzCd
                );
            },
            /**
               @todo It would be quite nice to get rid of this scoping
                     crutch eventually.
            */
            function (
                e, r, c, v, d, a, s, p, interval, locTzCd, callback
            ) {
                // save values in outer scope to avoid passing these
                // values through subsequent async.waterfal()
                // functions' scopes where they are not referenced at
                // all
                editable = e;
                cflag = c;
                vflag = v;
                dataType = d;
                agencyCode = a;
                siteNumber = s;
                parameterCode = p;
                during = interval;
                rndsup = r;

                callback(null);
            },
            /**
               @function
               @description Query
                            USGS-parameter-code-to-AQUARIUS-parameter
                            Web service here to obtain AQUARIUS
                            parameter from USGS parameter code.
               @callback
               @param {string} NWIS-RA authorization token.
               @param {function} callback async.waterfall() callback
                                 function.
            */
            function (callback) {
                try {
                    nwisRA.query(
                        {"parameters.PARM_ALIAS_CD": "AQNAME",
                         "parameters.PARM_CD": parameterCode},
                        options.log,
                        callback
                    );
                }
                catch (error) {
                    callback(error);
                    return;
                }
            },
            function (messageBody, callback) {
                var parameters;

                try {
                    parameters = JSON.parse(messageBody);
                }
                catch (error) {
                    callback(error);
                    return;
                }

                // load fields we need into something more coherent
                parameter = {
                    code: parameters.records[0].PARM_CD,
                    name: parameters.records[0].PARM_NM,
                    description: parameters.records[0].PARM_DS,
                    aquariusParameter: parameters.records[0].PARM_ALIAS_NM
                };

                callback(null);
            },
            //  get data and output to files
            ifAsync(dataTypeIsDV).then(
                dailyValues
            )
            .elseIf(dataTypeIsUV).then(
                unitValues
            )
        ],
            /**
               @description node-async error handler function.
               @callback
            */
            function (error) {
                if (error)
                    response.end(
                        "# " + packageName + ": " + error + '\n',
                        "ascii"
                    );
                else
                    response.end();
            }
        );
    }
); // aq2rdb

/**
   @description version endpoint, service request handler.
*/
httpdispatcher.onGet(
    '/' + packageName + '/version',
    /**
       @callback
    */
    function (request, response) {
        getVersion(function (version) {
            response.writeHeader(200, {"Content-Type": "text/html"});  
            response.end(
                '<?xml version="1.0" encoding="iso-8859-1"?>\n' +
                    '<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.1//EN"\n' +
                    '      "http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd">\n' +
                    '<html xmlns="http://www.w3.org/1999/xhtml">\n' +
                    '<head>\n' +
                    '  <meta http-equiv="content-type" ' +
                    '   content="text/html; charset=iso-8859-1" />\n' +
                    '  <title>aq2rdb Version</title>\n' +
                    '</head>\n' +
                    '<body>\n' +
                    '<p>' + version + '</p>\n' +
                    '</body>\n' +
                    '</html>\n'
            );
        });
    }
); // version

/**
   @function
   @description Service dispatcher.
   @private
   @param {object} request HTTP request object.
   @param {object} response HTTP response object.
*/ 
function handleRequest(request, response) {
    try {
        log(packageName + ".handleRequest().request.url", request.url);
        httpdispatcher.dispatch(request, response);
    }
    catch (error) {
        var prefix = "handleRequest().error";
        var msg = packageName + '.' + prefix + ": " + error;

        response.end("# " + msg, "ascii");
        log(prefix, error);
    }
}

try {
    // get set of successfully parsed command line options
    options = cli.parse();
}
catch (error) {
    /**
       @todo Error message is too vague here; could be more specific
             about which option(s) is/are unknown.
     */
    if (error.name === "UNKNOWN_OPTION") {
        console.log(packageName + ": error: Unknown option");
        console.log(cli.getUsage());
    }
    else {
        console.log(
            packageName +
                ": error: Error when parsing command-line arguments: "
                + error.name
        );
    }
    process.exit(1);
}

/**
   @class
   @classdesc NWIS-RA object prototype.
   @private
*/
var NWISRA = function (hostname, userName, password, log, callback) {
    var authentication;

    /**
       @method
       @description Get authentication token from NWIS-RA.
       @private
     */
    function authenticate(callback) {
        async.waterfall([
            function (cb) {
                try {
                    rest.querySecure(
                        hostname,
                        "POST",
                        {"content-type": "application/x-www-form-urlencoded"},
                        "/service/auth/authenticate",
                        {username: userName, password: password},
                        log,
                        cb
                    );
                }
                catch (error) {
                    cb(error);
                    return;
                }
            },
            function (messageBody, cb) {
                try {
                    authentication = JSON.parse(messageBody);
                }
                catch (error) {
                    cb(error);
                    return;
                }

                cb(null);
            }
        ],
            function (error) {
                if (error)
                    callback(error);
                else
                    callback(
                        null,
                        "Received NWIS-RA authentication token successfully"
                    );
            }
        );
    } // authenticate

    /**
       @method
       @description Make an NWIS-RA, HTTP GET query.
       @public
       @param {object} obj HTTP query parameter/value object.
       @param {boolean} log Enable console logging if true; no console
                        logging when false.
       @param {function} callback Callback function to call if/when
                         response is received.
    */
    this.query = function (obj, log, callback) {
        try {
            rest.querySecure(
                this.hostname,
                "GET",
                {"Authorization": "Bearer " + authentication.tokenId},
                "/service/data/view/parameters/json",
                obj,
                log,
                callback
            );
        }
        catch (error) {
            // Attempt to detect expired authentication token
            // error.
            if (error === "SyntaxError: Unexpected end of input") {
                async.waterfall([
                    function (callback) {
                        authenticate(callback); // try to refresh token
                        callback(null);
                    },
                    function (callback) {
                        // retry query one more time
                        rest.querySecure(
                            this.hostname,
                            "GET",
                            {"Authorization": "Bearer " +
                             authentication.tokenId},
                            "/service/data/view/parameters/json",
                            obj,
                            log,
                            callback
                        );
                    }
                ],
                    function (error) {
                        if (error)
                            callback(error);
                    }
                );
            }
            else
                callback(error);
            return;
        }
        // no callback call here; it is called from rest.querySecure()
        // above
    } // query

    // constructor
    this.hostname = hostname;
    this.userName = userName;
    this.password = password;
    this.log = log;
    authenticate(callback);

} // NWISRA

/**
   @description Check for "version" CLI option.
*/
if (options.version === true) {
    getVersion(function (version) { console.log(version); });
}
else {
    /**
       @description HTTP server to host the aq2rdb Web services.
       @private
       @type {object}
    */
    var server = http.createServer(handleRequest);
    var passwd = new Object();

    /**
       @function
       @description Attempt AQUARIUS handshaking to get authentication
                    token.
       @private
       @param {function} callback Callback function to call when
              complete.
    */
    function initAquarius(callback) {
        try {
            aquarius = new AQUARIUS(
                options.aquariusHostname,
                options.aquariusUserName,
                options.aquariusPassword, callback
            );
        }
        catch (error) {
            if (error) {
                callback(error);
                return;
            }
        }
    }

    async.waterfall([
        function (callback) {
            // if all prerequisite login information is missing on
            // command-line
            if (options.aquariusUserName === undefined &&
                options.aquariusPassword === undefined &&
                options.waterDataUserName === undefined &&
                options.waterDataPassword === undefined) {
                /** @todo need some logic here to find the encrypted
                    volume's mount point. */
                fs.readFile(
                    "/encryptedfs/aq2rdb-passwd.json",
                    function (error, json) {
                        if (error) {
                            callback(error);
                            return;
                        }

                        try {
                            passwd = JSON.parse(json);
                        }
                        catch (error) {
                            callback(error);
                            return;
                        }
                        callback(null);
                    }
                );
            }
            else {
                // use command-line information
                passwd.aquariusHostname = options.aquariusHostname;
                passwd.aquariusUserName = options.aquariusUserName;
                passwd.aquariusPassword = options.aquariusPassword;
                passwd.waterDataHostname = options.waterDataHostname;
                passwd.waterDataUserName = options.waterDataUserName;
                passwd.waterDataPassword = options.waterDataPassword;
                callback(null);
            }
        },
        function (callback) {
            // some server start-up, initialization tasks
            async.parallel([
                /**
                   @function
                   @description Attempt NWIS-RA handshaking to get
                                authentication token.
                */
                function (callback) {
                    try {
                        nwisRA = new NWISRA(
                            passwd.waterDataHostname,
                            passwd.waterDataUserName,
                            passwd.waterDataPassword, options.log,
                            callback
                        );
                    }
                    catch (error) {
                        if (error) {
                            callback(error);
                            return;
                        }
                    }
                    // no callback here; it is called from NWISRA()
                    // when complete
                },
                initAquarius,
                function (callback) {
                    fs.readFile("stat.json", function (error, json) {
                        if (error) {
                            callback(error);
                            return true;
                        }

                        try {
                            stat = JSON.parse(json);
                        }
                        catch (error) {
                            callback(error);
                            return;
                        }

                        callback(null, "Loaded stat.json");
                    });
                }
            ],
            function (error, results) {
                if (error) {
                    log(packageName, error);
                    return;
                }
                else {
                    async.each(
                        results,
                        function (message, callback) {
                            log(packageName, message);
                            callback(null);
                        }
                    );
                    /** @description Start listening for requests. */ 
                    server.listen(options.port, function () {
                        log(
                            packageName,
                            "Server listening on: http://localhost:" +
                                options.port.toString()
                        );
                        // Reconstruct the "aquarius" object every 59
                        // minutes to renew lease on authentication
                        // token. See
                        // https://nodejs.org/api/timers.html#timers_setinterval_callback_delay_arg
                        setInterval(
                            function () {
                                initAquarius(function (error, message) {
                                    if (error)
                                        log(error);
                                });
                            },
                            // call above function every 59 minutes:
                            59 * 60 * 1000
                        );
                    });
                }
           }); // async.parallel
        }
   ],
   function (error) {
       if (error.code === "ENOENT") {
           log(packageName,
               "No command-line credentials specified, and no " +
               "password file found at " +
               error.path);
       }
       else {
           log(packageName, error);
       }
   }
   ); // async.waterfall
}

/**
   @description Export module's private functions to test harness
                only.
   @see http://engineering.clever.com/2014/07/29/testing-private-functions-in-javascript-modules/
*/
if (process.env.NODE_ENV === "test") {
    module.exports._private = {
        AQUARIUS: AQUARIUS,
        cli: cli,
        docRequest: docRequest,
        dvTableRow: dvTableRow,
        handle: handle,
        jsonParseErrorMessage: jsonParseErrorMessage,
        nwisVersusIANA: nwisVersusIANA,
        options: options
    };
}
