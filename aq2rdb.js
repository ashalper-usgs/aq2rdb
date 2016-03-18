/**
 * @fileOverview A Web service to map AQUARIUS, time series data
 *               requests to USGS-variant RDB files.
 *
 * @author <a href="mailto:ashalper@usgs.gov">Andrew Halper</a>
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
*/
var packageName = path.basename(process.argv[1]).slice(0, -3);

/**
   @description Domain of supported command line arguments.
   @see https://www.npmjs.com/package/command-line-args#synopsis
*/
var cli = commandLineArgs([
    /**
       @description Print version and exit.
    */
    {name: "version", alias: 'v', type: Boolean, defaultValue: false},
    /**
       @description Enable logging.
    */
    {name: "log", alias: 'l', type: Boolean, defaultValue: false},
    /**
       @description TCP/IP port that aq2rdb will listen on.
    */
    {name: "port", alias: 'p', type: Number, defaultValue: 8081},
    /**
       @description DNS name of AQUARIUS Web service host.
    */
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
    /**
       @description DNS name of aquarius-token Web service host.
    */
    {name: "aquariusTokenHostname", alias: 't', type: String,
     defaultValue: "localhost"},
    /**
       @description DNS name of USGS Water Services Web service host.
    */
    {name: "waterServicesHostname", type: String,
     defaultValue: "waterservices.usgs.gov"},
    /**
       @description DNS name of USGS NWIS service host.
    */
    {name: "waterDataHostname", type: String,
     defaultValue: "nwisdata.usgs.gov"},
    /**
       @description USGS NWIS service host, service account user name.
    */
    {name: "waterDataUserName", type: String},
    /**
       @description DNS name of USGS NWIS service host.
    */
    {name: "waterDataPassword", type: String}
]);

var aquarius;               // AQUARIUS object
var nwisRA;                 // NWIS-RA object (see "NWISRA" prototype)

/**
   @description A mapping of select NWIS time zone codes to IANA time
                zone names (referenced by moment-timezone
                module). This is not a complete enumeration of the
                time zones defined in the NWIS TZ table, but the time
                zone abbreviations known (presently) to be related to
                SITEFILE sites in NATDB.
   @constant
*/
var tzName = Object();
/**
   @todo Need to check moment.tz() for "N"
*/
tzName['AFT'] =   {N: 'Asia/Kabul', Y: 'Asia/Kabul'};
tzName['AKST'] =  {N: 'Etc/GMT-9',  Y: 'America/Anchorage'};
tzName['AST'] =   {N: 'Etc/GMT-4',  Y: 'America/Glace_Bay'};
tzName['AWST'] =  {N: 'Etc/GMT+4',  Y: 'Australia/Perth'};
tzName['BT'] =    {N: 'Etc/GMT+3',  Y: 'Asia/Baghdad'};
tzName['CST'] =   {N: 'Etc/GMT-6',  Y: 'America/Chicago'};
tzName['DST'] =   {N: 'Etc/GMT+1',  Y: 'Etc/GMT+1'};
tzName['EET'] =   {N: 'Etc/GMT+2',  Y: 'Europe/Athens'};
tzName['EST'] =   {N: 'Etc/GMT-5',  Y: 'America/New_York'};
tzName['GMT'] =   {N: 'Etc/GMT+0',  Y: 'Europe/London'};
tzName['GST'] =   {N: 'Etc/GMT+10', Y: 'Pacific/Guam'};
tzName['HST'] =   {N: 'Etc/GMT-10', Y: 'HST'};
// NWIS's "International Date Line, East":
tzName['IDLE'] =  {N: 'Etc/GMT+12', Y: 'Etc/GMT+12'};
// NWIS's "International Date Line, West":
tzName['IDLW'] =  {N: 'Etc/GMT-12', Y: 'Etc/GMT-12'};
tzName['JST'] =   {N: 'Etc/GMT+9',  Y: 'Asia/Tokyo'};
tzName['MST'] =   {N: 'America/Phoenix',  Y: 'America/Denver'};
// moment-timezone has no support for UTC-03:30 (in the context of
// Northern Hemisphere summer), which would be the mapping of NWIS'
// (NST,N) [i.e., "Newfoundland Standard Time, local time not
// acknowledged"] SITEFILE predicate...
tzName['NST'] =   {N: 'UTC-03:30',  Y: 'America/St_Johns'};
tzName['NZT'] =   {N: 'Etc/GMT+12', Y: 'NZ'};
tzName['PST'] =   {N: 'Etc/GMT-8',  Y: 'America/Los_Angeles'};
// ...similarly, moment-timezone has no support for UTC+09:30 (in the
// context of Southern Hemisphere summer), which would be the mapping
// of NWIS' (SAT,N) [i.e., "South Australian Standard Time, local time
// not acknowledged"]
tzName['SAT'] =   {N: 'UTC+09:30',  Y: 'Australia/Adelaide'};
tzName['UTC'] =   {N: 'Etc/GMT+0',  Y: 'Etc/GMT+0'};
tzName['WAST'] =  {N: 'Etc/GMT+7',  Y: 'Australia/Perth'};
tzName['WAT'] =   {N: 'Etc/GMT+1',  Y: 'Africa/Bangui'};
tzName['ZP-11'] = {N: 'Etc/GMT-11', Y: 'Etc/GMT-11'};
tzName['ZP11'] =  {N: 'Etc/GMT+11', Y: 'Etc/GMT+11'};
tzName['ZP4'] =   {N: 'Etc/GMT+4',  Y: 'Etc/GMT+4'};
tzName['ZP5'] =   {N: 'Etc/GMT+5',  Y: 'Etc/GMT+5'};
tzName['ZP6'] =   {N: 'Etc/GMT+6',  Y: 'Etc/GMT+6'};

/**
   @description Public functions.
*/
var aq2rdb = module.exports = {
    /**
       @function Convert AQUARIUS TimeSeriesPoint.Timestamp string to
                 a common NWIS date format.
       @public
       @param {string} timestamp AQUARIUS Timestamp string to convert.
    */
    toNWISDateFormat: function (timestamp) {
        var date = new Date(timestamp);

        return timestamp.split('T')[0].replace(/-/g, '');
    },

    /**
       @function Convert AQUARIUS TimeSeriesPoint.Timestamp string to
                 a common NWIS time format.
       @public
       @param {string} timestamp AQUARIUS Timestamp string to convert.
    */
    toNWISTimeFormat: function (timestamp) {
        var date = new Date(timestamp);

        return timestamp.split(/[T.]/)[1].replace(/:/g, '');
    },

    /**
       @function Convert AQUARIUS TimeSeriesPoint.Timestamp string to
                 a common NWIS datetime format.
       @public
       @param {string} timestamp AQUARIUS Timestamp string to convert.
    */
    toNWISDatetimeFormat: function (timestamp) {
        return aq2rdb.toNWISDateFormat(timestamp) +
            aq2rdb.toNWISTimeFormat(timestamp);
    }
}; // public functions

/**
   @function This module's logging function, mostly for convenience
             purposes.
   @private
   @param {string} message Log message.
*/ 
function log(prefix, message) {
    if (options.log)
        console.log(prefix + ": " + message);
} // log

/**
   @function Error handler.
   @private
   @param {object} error "Error" object.
   @param {object} response IncomingMessage object created by Node.js
          http.Server.
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
           @todo It would be nice to refine this. Too generic now.
        */
        statusCode = 404;
    }
    else {
        statusMessage = '# ' + packageName + ': ' + error.message;
        /**
           @default HTTP error status code.
           @todo It would be nice to refine this. Too generic now.
        */
        statusCode = 404;
    }

    return [statusCode, statusMessage];
} // handle

/**
   @function Error messager for JSON parse errors.
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
   @description LocationIdentifier object prototype.
   @class
   @private
*/
var LocationIdentifier = function (text) {
    var text = text;

    /**
       @method Return agency code.
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
       @method Return site number.
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
       @method Return string representation of this object.
    */
    this.toString = function () {
        return text;
    }

} // LocationIdentifier

/**
   @function Check for documentation request, and serve documentation
             if appropriate.
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
            'doc/' + path.basename(servicePath) + '.html',
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
   @function Create RDB, DV table row.
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
    // TIME column will always be empty for daily values
    var row = aq2rdb.toNWISDateFormat(timestamp) + '\t\t';

    if (value.Display !== undefined)
        row += value.Display;

    row += '\t';

    /**
       @author <a href="mailto:sbarthol@usgs.gov">Scott Bartholoma</a>

       @since 2015-09-29T10:57-07:00

       Remark will have to be derived from the Qualifier section of
       the response. It will have begin and end dates for various
       qualification periods.
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
        row += '\t';
    });

    /**
       @author <a href="mailto:sbarthol@usgs.gov">Scott Bartholoma</a>

       @since 2015-09-29T10:57-07:00
      
       I think some of what used to be flags are now
       Qualifiers. Things like thereshold [sic] exceedances [sic]
       (high, very high, low, very low, rapid increace/decreast [sic],
       etc.). The users might want you to put something in that column
       for the Method and Grade sections of the response as well
    */
    row += '\t' +

    /**
       @author <a href="mailto:sbarthol@usgs.gov">Scott Bartholoma</a>

       @since 2015-09-29T10:57-07:00
      
       Type I would put in something like "R" for raw and "C" for
       corrected depending on which get method was used. That is
       similar to what C (computed) and E (Edited) meant for DV data
       in Adaps.  We don't explicitly have the Meas, Edit, and Comp UV
       types anymore, they are separate timeseries in AQUARIUS.
    */
    '\tC\t' + qa + '\n';

    return row;
} // dvTableRow

/**
   @function Patch up some obscure incompatibilities between NWIS's
             site time offset predicate and IANA time zone data (used
             by moment-timezone).
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

var AQUARIUS = function (hostname, userName, password, callback) {
    if (hostname === undefined) {
        callback('AQUARIUS(): Required field "hostname" not found');
        return;
    }

    if (hostname === '') {
        callback('AQUARIUS(): Required field "hostname" must have a value');
        return;
    }

    this.hostname = hostname;

    if (userName === undefined) {
        callback('AQUARIUS(): Required field "userName" not found');
        return;
    }

    if (userName === '') {
        callback('AQUARIUS(): Required field "userName" must have a value');
        return;
    }

    if (password === undefined) {
        callback('AQUARIUS(): Required field "password" not found');
        return;
    }

    if (password === '') {
        callback('AQUARIUS(): Required field "password" must have a value');
        return;
    }

    var token;

    /**
       @description GetAQToken service response callback.
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
            callback(null);
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
       @method AQUARIUS authentication token.
     */
    this.token = function () {
        return token;
    }

    /**
       @method Call AQUARIUS GetLocationData Web service.
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
           @description Handle GetTimeSeriesDescriptionList service
                        invocation errors.
        */
        request.on("error", function (error) {
            callback(error);
            return;
        });

        request.end();
    } // getLocationData

    /**
       @method Call AQUARIUS GetTimeSeriesCorrectedData Web service.
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
        // call, so they're not passed in
        parameters["token"] = token;
        parameters["format"] = "json";

        var path = "/AQUARIUS/Publish/V2/GetTimeSeriesCorrectedData?" +
            querystring.stringify(parameters);

        log(
            packageName + ".AQUARIUS.getTimeSeriesCorrectedData()::URL",
            "http://" + hostname + path
        );

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
       @method Parse AQUARIUS TimeSeriesDataServiceResponse received
               from GetTimeSeriesCorrectedData service.
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
       @method Distill a set of time series descriptions into
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
    this.distill = function (
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
   @function Parse aq2rdb?t=UV endpoint's fields. Proceed to next
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
   @function Node.js emulation of a proper subset of legacy NWIS,
             NWF_RDB_OUT() Fortran subroutine: "Top-level routine for
             outputting rdb format data".
   @author <a href="mailto:ashalper@usgs.gov">Andrew Halper</a>
   @author <a href="mailto:sbarthol@usgs.gov">Scott Bartholoma</a>
   @param {string} dataType Rating (time series?) type.
   @param {Boolean} rndsup Rounding suppression flag.
   @param {Boolean} wyflag Water year flag.
   @param {Boolean} cflag Computed flag.
   @param {Boolean} vflag Verbose dates and times flag.
   @param {string} agencyCode Site agency code.
   @param {string} siteNumber Site number (a.k.a. "site ID").
   @param {string} parameterCode Parameter code [and regrettably,
          sometimes DD number in NWF_RDB_OUT()]
   @param {string} instat Statistic code.
   @param {string} begdat Begin date/datetime.
   @param {string} enddat End date/datetime.
   @param {string} inLocTzCd Location time zone code.
   @param {string} titlline
*/
function rdbOut(
    dataType, rndsup, wyflag, cflag, vflag, agencyCode, siteNumber,
    parameterCode, instat, begdat, enddat, locTzCd, titlline, callback
) {
    var datatyp, stat, uvtyp, interval;
    var uvtypPrompted = false;

    if (locTzCd === undefined) locTzCd = 'LOC';

    /**
       @todo might not be needed:

       if (intrans(1:1).EQ.' ')
       transport_cd = ' '
       sensor_type_id = NW_NI4
       ELSE
       transport_cd = intrans(1:1)
       CALL s_upcase (transport_cd,1)
       sensor_type_id = 0
       END IF
    */

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

    log(packageName + ".rdbOut().parameterCode", parameterCode);

    // further processing depends on data type

    if (dataType === 'DV') { // convert stat to 5 characters
        if (instat === undefined) {
            sopt[7] = '1';
        }
        else {
            if (5 < instat.length)
                stat = instat.substring(0, 5);
            else
                stat = instat;
            stat = sprintf("%5s", stat).replace(' ', '0');
        }
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
            interval = new adaps.IntervalDay(begdat, enddat, wyflag);
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
            interval = new adaps.IntervalSecond(begdat, enddat, wyflag);
        }

    }

    // This is where, formerly, NWF_RDB_OUT():
    // 
    //    get PRIMARY DD that goes with parm if parm supplied
    //
    // Since the algorithmic equivalent is now deep within
    // the bowels of fuvrdbout(), it is no longer
    // here. This comment is just a reminder that it used
    // to be here, in case it is later discovered that the
    // primary identification is necessary before
    // fuvrdbout() does it.

    // retrieving measured uvs and transport_cd not
    // supplied, prompt for it
    if (uvtypPrompted && dataType === "UV" &&
        (uvtyp === 'M' || uvtyp === 'N') &&
        transport_cd === undefined) {
        /**
           @todo Convert to callback error?
        */
        /*
          nw_query_meas_uv_type(agencyCode, siteNumber, ddid, begdtm,
          enddtm, loc_tz_cd, transport_cd,
          sensor_type_id, *998)
          if (transport_cd === undefined) {
          WRITE (0,2150) agencyCode, siteNumber, ddid
          2150      FORMAT (/,"No MEASURED UV data for station "",A5,A15,
          "", DD "',A4,'".  Aborting.",/)
          return irc;
          END IF
        */
    }

    callback(
        null, false, rndsup, cflag, vflag, dataType, agencyCode,
        siteNumber, parameterCode, interval, locTzCd
    );
} // rdbOut

/**
   @function Get a TimeSeriesDescription object from AQUARIUS.
   @private
   @param {string} token AQUARIUS authentication token.
   @param {string} agencyCode USGS agency code.
   @param {string} siteNumber USGS site number.
   @param {string} AQUARIUS parameter.
   @param {function} outerCallback Callback function to call when complete.
*/
function getTimeSeriesDescription(
    token, agencyCode, siteNumber, parameter, computationIdentifier,
    computationPeriodIdentifier, outerCallback
) {
    var locationIdentifier, timeSeriesDescription;

    async.waterfall([
        /**
           @function Query AQUARIUS GetTimeSeriesDescriptionList
                     service to get list of AQUARIUS, time series
                     UniqueIds related to aq2rdb, GetUVTable location
                     and parameter.
           @callback
           @param {function} callback async.waterfall() callback
           function.
        */
        function (callback) {
            // make (agencyCode,siteNo) digestible by AQUARIUS
            locationIdentifier = (agencyCode === "USGS") ?
                siteNumber : siteNumber + '-' + agencyCode;

            var obj = {
                token: aquarius.token(),
                format: "json",
                LocationIdentifier: locationIdentifier,
                Parameter: parameter,
                ComputationPeriodIdentifier: computationPeriodIdentifier,
                // not sure what this does:
                ExtendedFilters: "[{FilterName:ACTIVE_FLAG,FilterValue:Y}]"
            };

            if (computationIdentifier)
                obj["ComputationIdentifier"] = computationIdentifier;

            try {
                rest.query(
                    aquarius.hostname,
                    "GET",
                    undefined,  // HTTP headers
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
        },
        /**
           @function Receive response from AQUARIUS
                     GetTimeSeriesDescriptionList, then parse list of
                     related TimeSeriesDescriptions to query AQUARIUS
                     GetTimeSeriesCorrectedData service.
           @callback
           @param {string} messageBody Message body part of HTTP
                  response from GetTimeSeriesDescriptionList.
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
           @function Check for zero TimeSeriesDescriptions returned
                     from AQUARIUS Web service query above.
           @callback
        */
        function (timeSeriesDescriptions, callback) {
            if (timeSeriesDescriptions.length === 0) {
                /**
                   @todo Might be more helpful to have "...found at
                   <URL>" in this message.
                */
                callback(
                    "No time series description list found at " +
                        url.format({
                            protocol: "http",
                            host: aquarius.hostname,
                            pathname:
                            "/AQUARIUS/Publish/V2/GetTimeSeriesDescriptionList",
                            query:
                            {token: aquarius.token(),
                             format: "json",
                             LocationIdentifier: locationIdentifier,
                             Parameter: parameter,
                             ComputationIdentifier: computationIdentifier,
                             ComputationPeriodIdentifier:
                                computationPeriodIdentifier,
                             ExtendedFilters: extendedFilters}
                        })
                );
                return;
            }

            callback(null, timeSeriesDescriptions);
        },
        /**
           @function For each AQUARIUS time series description, weed
                     out non-UV, and non-primary ones.
           @callback
        */
        function (timeSeriesDescriptions, callback) {
            /**
               @todo Now that AQUARIUS accepts
                     ComputationPeriodIdentifier=Points [see
                     url.format() call above], some/all of
                     this might no longer be necessary.
            */
            async.filter(
                timeSeriesDescriptions,
                function (timeSeriesDescription, callback) {
                    if (timeSeriesDescription.ComputationPeriodIdentifier
                         === "Points") {
                            callback(true);
                        }
                    else {
                        callback(false);
                    }
                },
                /**
                   @todo If aquarius.distill() can't find a primary
                   record:

                   WRITE (0,2120) agencyCode, siteNumber, parm
                   2120    FORMAT (/,"No PRIMARY DD for station "",A5,A15,
                   "", parm "',A5,'".  Aborting.",/)
                */
                function (uvTimeSeriesDescriptions) {
                    timeSeriesDescription = aquarius.distill(
                        uvTimeSeriesDescriptions, locationIdentifier,
                        callback
                    );
                }
            );
            callback(null);
        },
    ],
        function(error) {
            if (error)
                outerCallback(error);
            else
                outerCallback(null, timeSeriesDescription);
        }
    );
} // getTimeSeriesDescription

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
            log(
                packageName +
                    "getVersion().fs.readFile(\"package.json\", (error))",
                error
            );
            return;
        }

        callback(pkg.version);  // pass version number to callback function
    });
} // getVersion

/**
   @function Convert NWIS datetime format to ISO format for digestion
             by AQUARIUS REST query. Offset times from time zone of
             site to UTC to get correct results.
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
                during.from,
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
                during.to,
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
   @description GetDVTable endpoint service request handler.
*/
httpdispatcher.onGet(
    '/' + packageName + '/GetDVTable',
    /**
       @callback
    */
    function (request, response) {
        var field, token, locationIdentifier, timeSeriesDescription;
        var remarkCodes;

        /**
           @see https://github.com/caolan/async
        */
        async.waterfall([
            /**
               @function Check for documentation request.
               @callback
            */
            function (callback) {
                if (docRequest(request.url, '/aq2rdb/GetDVTable',
                               response, callback))
                    return;
                callback(null);
            },
            /**
               @function Parse fields and values in GetDVTable URL.
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

                // proceed to next waterfall
                callback(null);
            },
            /**
               @function Query AQUARIUS GetTimeSeriesDescriptionList
                         service to get list of AQUARIUS, time series
                         UniqueIds related to aq2rdb, GetDVTable
                         location and parameter.
               @callback
               @param {function} callback async.waterfall() callback
                      function.
            */
            function (callback) {
                var obj =
                    {token: aquarius.token(), format: "json",
                     LocationIdentifier: locationIdentifier.toString(),
                     Parameter: field.Parameter,
                     ComputationPeriodIdentifier: "Daily",
                     ExtendedFilters:
                     "[{FilterName:ACTIVE_FLAG,FilterValue:Y}]"};

                if (field.ComputationIdentifier !== undefined)
                    obj.ComputationIdentifier = field.ComputationIdentifier;
                
                try {
                    rest.query(
                        aquarius.hostname,
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
            },
            /**
               @function Receive response from AQUARIUS
                         GetTimeSeriesDescriptionList, then parse list
                         of related TimeSeriesDescriptions to query
                         AQUARIUS GetTimeSeriesCorrectedData service.
               @callback
               @param {string} messageBody Message body part of
                      HTTP response from GetTimeSeriesDescriptionList.
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
               @function For each AQUARIUS time series description,
                         query GetTimeSeriesCorrectedData to get
                         related daily values.
               @callback
            */
            function (timeSeriesDescriptions, callback) {
                timeSeriesDescription =
                    aquarius.distill(
                        timeSeriesDescriptions, locationIdentifier, callback
                    );

                log(packageName +
                    ".httpdispatcher.onGet().GetDVTable.locationIdentifier",
                    locationIdentifier);
                
                callback(
                    null, options.waterServicesHostname,
                    locationIdentifier.agencyCode(),
                    locationIdentifier.siteNumber(), options.log
                );
            },
            site.request,
            site.receive,
            /**
               @function Write RDB header and heading.
               @callback
            */
            function (site, callback) {
                async.series([
                    /**
                       @function Write HTTP response header.
                       @callback
                    */
                    function (callback) {
                        response.writeHead(
                            200, {"Content-Type": "text/plain"}
                        );
                        callback(null);
                    },
                    /**
                       @function Write RDB header to HTTP response.
                       @callback
                    */
                    function (callback) {
                        /**
                           @todo some actual parameters here likely need to
                           be corrected
                         */
                        rdb.header(
                            "NWIS-I DAILY-VALUES", site,
                            subLocationIdentifer, parameter, range,
                            callback
                        );
                    },
                    /**
                       @function Write RDB body to HTTP response.
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
                       @function Write RDB heading (a different thing
                                 than RDB header, above) to HTTP
                                 response.
                       @callback
                    */
                    function (callback) {
                        response.write(
                            'DATE\tTIME\tVALUE\tREMARK\tFLAGS\tTYPE\tQA\n' +
                                '8D\t6S\t16N\t1S\t32S\t1S\t1S\n', 'ascii'
                        );
                        callback(null);
                    }
                ]);
                callback(null);
            },
            /**
               @function Request remark codes from AQUARIUS.
               @callback
               @todo This is fairly kludgey, because remark codes
                     might not be required for every DV interval; try
                     to nest in a conditional eventually.
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
               @function Receive remark codes from AQUARIUS.
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
                    /**
                       @callback
                    */
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
               @function Query AQUARIUS GetTimeSeriesCorrectedData
                         to get related daily values.
               @callback
            */
            function (callback) {               
                try {
                    aquarius.getTimeSeriesCorrectedData(
                        {TimeSeriesUniqueId: timeSeriesDescription.UniqueId,
                         QueryFrom: field.QueryFrom,
                         QueryTo: field.QueryTo},
                        callback
                    );
                }
                catch (error) {
                    callback(error);
                    return;
                }
            },
            aquarius.parseTimeSeriesDataServiceResponse,
            /**
               @function Write each RDB row to HTTP response.
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
            }],
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
        var token, dataType, agencyCode, siteNumber, locationIdentifier;
        var waterServicesSite;
        /**
           @todo Need to check downstream depedendencies in aq2rdb
                 endpoint's async.waterfall function for presence of
                 former "P" prefix of this value.
        */
        var parameterCode;
        var parameter, extendedFilters;
        var timeSeriesDescription, during, editable, cflag, vflag;
        var rndsup, locTzCd;

        log(packageName + ".httpdispatcher.onGet(/" + packageName +
            ", (request))", request);

        /**
           @function node-if-async predicate function, called by
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
           @function node-if-async predicate function, called by
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
           @function A Node.js emulation of legacy NWIS, FDVRDBOUT()
                     Fortran subroutine: "Write DV data in rdb FORMAT"
                     [sic].
        */
        function fdvrdbout(callback) {
            var parameters = Object();
            // many/most of these are artifacts of the legacy code,
            // and probably won't be needed:
            var dvWaterYr, tunit;
            var cval, cdate, odate, outlin, rndary = ' ';
            var rndparm, rnddd, cdvabort = ' ', bnwisdt, enwisdt;
            var bnwisdtm, enwisdtm, bingdt, eingdt, temppath;
            var nullval = '**NULL**', nullrd = ' ', nullrmk = ' ';
            var nulltype = ' ', nullaging = ' ';
            var first = true;
            var pcode;
            var timeSeriesDescription;

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
                    parameters = appendIntervalSearchCondition(
                        parameters, during,
                        waterServicesSite.tzCode,
                        "00000000", "99999999",
                        callback
                    );

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

                    callback(null);
                },
                /**
                   @todo this might be obsolete
                */
                function (callback) {
                    // DV abort limit defaults to 120 minutes
                    cdvabort = '120';

                    /**
                       @todo get the DV abort limit
                       @see
                          watstore/adaps/adrsrc/tsing_lib/nw_db_retr_dvabort.sf

                       if (dbRetrDVAbort(ddagny, ddstid, ddid, bnwisdtm,
                           enwisdtm, dvabort)) {
                           cdvabort = sprintf("%6d", dvabort);
                       }
                    */
                    callback(null);
                },
                function (callback) {
                    /**
                       @todo get stat information

                       irc = s_statck(stat);

                       if (irc !== 0)
                          ssnam = '*** INVALID STAT ***';
                    */
                    callback(
                        null, token, agencyCode, siteNumber,
                        parameter.aquariusParameter, undefined,
                        "Daily"
                    );
                },
                getTimeSeriesDescription,
                function (tsd, callback) {
                    timeSeriesDescription = tsd; // set variable in outer scope
                    callback(null);
                },
                function (callback) {
                    async.waterfall([
                        function (callback) {
                            // write the header records
                            rdb.header(
                                "NWIS-I DAILY-VALUES",
                                (editable) ? "YES" : "NO",
                                waterServicesSite,
                                timeSeriesDescription.SubLocationIdentifer,
                                parameter,
                                /**
                                   @todo This is pragmatically
                                         hard-coded now, but there is
                                         a relationship to "cflag"
                                         value above.
                                */
                                {code: 'C', name: "COMPUTED"},
                                {start: during.from, end: during.to}
                            );
                            callback(null);
                        },
                        function (callback) {
                            /**
                               @todo write database info
                            */
                            rdbDBLine(funit);
                            callback(null);
                        },
                        function (callback) {
                            /**
                               @todo write Location info

                               At 8:30 AM, Feb 16th, 2016, Wade Walker
                               <walker@usgs.gov> said:

                               sublocation is the AQUARIUS equivalent
                               of ADAPS location. It is returned from
                               any of the
                               GetTimeSeriesDescriptionList... methods
                               or for GetFieldVisitData method
                               elements where sublocation is
                               appropriate. GetSensorsAndGages will
                               also return associated
                               sublocations. They're basically just a
                               shared attribute of time series,
                               sensors and gages, and field readings,
                               so no specific call for them, they're
                               just returned with the data they're
                               applicable to. Let me know if you need
                               something beyond that.

                               rdbWriteLocInfo(funit, dd_id);
                            */
                            callback(null);
                        },
                        function (callback) {
                            // write DD info
                            funit.write(
                                '# //PARAMETER CODE="' +
                                    pcode.substr(1, 5) + '" SNAME = "' +
                                    psnam + '"\n' +
                                    '# //PARAMETER LNAME="' + plname +
                                    '"\n' +
                                    '# //STATISTIC CODE="' +
                                    scode.substr(1, 5) + '" SNAME="' +
                                    ssnam + '"\n' +
                                    '# //STATISTIC LNAME="' + slname + '"\n',
                                "ascii"
                            );
                            callback(null);
                        },
                        function (callback) {
                            // write DV type info
                            if (compdv) {
                                funit.write(
                                    '# //TYPE NAME="COMPUTED" ' +
                                      'DESC = "COMPUTED DAILY VALUES ONLY"\n',
                                    "ascii"
                                )
                            }
                            else {
                                funit.write(
                                    '# //TYPE NAME="FINAL" ' +
                                  'DESC = "EDITED AND COMPUTED DAILY VALUES"\n',
                                    "ascii"
                                )
                            }
                            callback(null);
                        },
                        function (callback) {
                            /**
                               @todo write data aging information
                               rdbWriteAging(
                               funit, dbnum, dd_id, begdate, enddate
                               );
                            */
                            callback(null);
                        },
                        function (callback) {
                            // write editable range
                            funit.write(
                                '# //RANGE START="' + begdate +
                                    '" END="' + enddate + '"\n',
                                "ascii"
                            )
                            callback(null);
                        },
                        function (callback) {
                            // write single site RDB column headings
                            funit.write(
                                "DATE\tTIME\tVALUE\tPRECISION\t" +
                                    "REMARK\tFLAGS\tTYPE\tQA\n",
                                "ascii"
                            );
                            callback(null);
                        },
                        function (callback) {
                            var dtcolw;
                            
                            // if verbose, Excel-style format
                            if (vflag) {
                                dtcolw = '10D';     // "mm/dd/yyyy" 10 chars
                            }
                            else {
                                dtcolw = '8D';      // "yyyymmdd" 8 chars
                            }

                            // WRITE (funit,'(20A)') outlin(1:23+dtlen)
                            funit.write(
                                dtcolw + "\t6S\t16N\t1S\t1S\t32S\t1S\t1S",
                                "ascii"
                            );
                            callback(null);
                        }
                    ]); // async.waterfall

                    if (first) {
                        async.waterfall([
                            function (callback) {
                                /**
                                   @todo write "with keys" rdb column headings
                                   WRITE (funit,'(20A)')
                                   *           '# //FILE TYPE="NWIS-I DAILY-VALUES" ',
                                   *           'EDITABLE=NO'
                                   */
                                callback(null);
                            },
                            function (callback) {
                                // write database info
                                nw_rdb_dbline(funit);
                                callback(null);
                            },
                            function (callback) {
                                funit.write(
                                    "AGENCY\tSTATION\tDD\tPARAMETER\tSTATISTIC\tDATE\t" +
                                        "TIME\tVALUE\tPRECISION\tREMARK\tFLAGS\tTYPE\tQA\n",
                                    "ascii"
                                );
                                callback(null);
                            },
                            function (callback) {
                                var dtcolw;
                                
                                // if verbose, Excel-style format
                                if (vflag) {
                                    dtcolw = "10D";  // "mm/dd/yyyy" 10 chars
                                }
                                else {
                                    dtcolw = "8D";   // "yyyymmdd" 8 chars
                                }

                                funit.write(
                                    "5S\t15S\t4S\t5S\t5S\t" + dtcolw +
                                        "\t6S\t16N\t1S\t1S\t32S\t1S\t1S\n",
                                    "ascii"
                                );
                                
                                first = false;
                                callback(null);
                            }
                        ]);
                    }
                    callback(null);
                },
                function (callback) {
                    // Setup begin date
                    if (begdate === '00000000') {
                        if (! nw_db_retr_dv_first_yr(dd_id, stat, dvWaterYr)) {
                            return nw_get_error_number();
                        }
                        /**
                           WRITE (bnwisdt,2030) dvWaterYr - 1
                           2030       FORMAT (I4.4,'1001')
                        */
                        bnwisdt = sprintf("%4d1001", dvWaterYr - 1);
                    }

                    // validate and load begin date into ingres FORMAT
                    if (! nw_cdt_ok(bnwisdt))
                        return 3;
                    else
                        nw_dt_nwis2ing(bnwisdt, bingdt)

                    // Setup end date
                    if (enddate === '99999999') {
                        if (! nw_db_retr_dv_last_yr(dd_id, stat, dvWaterYr)) {
                            return nw_get_error_number();
                        }
                        /*
                          WRITE (enwisdt,2040) dvWaterYr
                          2040       FORMAT (I4.4,'0930')
                        */
                        enwisdt = sprintf("%4d0930", dvWaterYr);
                    }

                    // validate and load end date into ingres FORMAT
                    if (! nw_cdt_ok(enwisdt))
                        return 3;
                    else
                        nw_dt_nwis2ing(enwisdt, eingdt);

                    odate = bnwisdt;
                    if (! compdv) {
                        /**
                           @todo

                           stmt = 'SELECT dvd.dv_dt, dvd.dv_va, dvd.dv_rd, ' //
                           *                    'dvd.dv_rmk_cd, dvd.dv_type_cd, ' //
                           *                    'dvd.data_aging_cd FROM ' //
                           *                dv_data_name(1:ldv_data_name) // ' dvd, ' //
                           *                dv_name(1:ldv_name) // ' dv ' //
                           *             'WHERE dv.dd_id = ' // cdd_id // ' AND ' //
                           *                   'dv.stat_cd = ''' // stat // ''' AND ' //
                           *                   'dvd.dv_id = dv.dv_id AND ' // 
                           *                   'dvd.dv_dt >=  ''' // bingdt // ''' AND ' // 
                           *                   'dvd.dv_dt <=  ''' // eingdt // ''' '  // 
                           *             'ORDER BY dvd.dv_dt'
                           */
                    }
                    else {
                        /*
                          @todo

                          stmt = 'SELECT dvd.dv_dt, dvd.dv_va, dvd.dv_rd,' //
                          *               'dvd.dv_rmk_cd, dvd.dv_type_cd,' //
                          *               'dvd.data_aging_cd FROM ' //
                          *             dv_data_name(1:ldv_data_name) // ' dvd, ' //
                          *             dv_name(1:ldv_name) // ' dv ' //
                          *             'WHERE dv.dd_id = ' // cdd_id // ' AND ' //
                          *             'dv.stat_cd = ''' // stat // ''' AND  ' // 
                          *             'dvd.dv_id = dv.dv_id AND ' //
                          *             'dvd.dv_type_cd = ''C'' AND ' //
                          *             'dvd.dv_dt >=  ''' // bingdt // ''' AND ' //
                          *             'dvd.dv_dt <=  ''' // eingdt // ''' ' // 
                          *             ' UNION ' //
                          *             'SELECT dvf.dv_dt, dvf.dv_va, dvf.dv_rd, ' //
                          *               'dvf.dv_rmk_cd, dvf.dv_type_cd, ' //
                          *               'dvf.data_aging_cd FROM ' //
                          *             dv_diff_name(1:ldv_diff_name) // ' dvf, ' //
                          *             dv_name(1:ldv_name) // ' dv ' //
                          *             'WHERE dv.dd_id = ' // cdd_id // ' AND ' //
                          *             'dv.stat_cd = ''' // stat // ''' AND ' // 
                          *             'dvf.dv_id = dv.dv_id AND ' //
                          *             'dvf.dv_type_cd = ''C'' AND ' //
                          *             'dvf.dv_dt >=  ''' // bingdt // ''' AND ' //
                          *             'dvf.dv_dt <=  ''' // eingdt // ''' ' //
                          *             ' ORDER BY dv_dt'
                          */
                    }

                    // TODO:
                    /*
                      EXEC SQL PREPARE pstmt FROM :stmt
                      nw_sql_error_handler ('fdvrdbout', 'prepare',
                      *        'Retrieving DV data', rowcount, irc)
                      if (irc === 0) {
                      EXEC SQL OPEN cur_stmt
                      nw_sql_error_handler ('fdvrdbout', 'opencurs',
                      *           'Retrieving DV data', rowcount, irc)
                      if (irc === 0) {
                      DO
                      EXEC SQL FETCH cur_stmt INTO
                      *                 :dv_dt, :dv_va:dv_va_null, :dv_rd:dv_rd_null,
                      *                 :dv_rmk_cd, :dv_type_cd, :data_aging_cd

                      if (nw_no_data_found()) EXIT
                      
                      nw_sql_error_handler ('fdvrdbout', 'fetch',
                      *                 'Retrieving DV data', rowcount, irc)
                      if (irc !== 0) EXIT

                      nw_dt_ing2nwis (dv_dt, cdate)
                      10               if (odate .LT. cdate) {
                      WRITE (tunit) odate, nullval, nullrd, nullrmk, 
                      *                    nulltype, nullaging
                      nw_dtinc (odate, 1)
                      GO TO 10
                      }
                    */
                    // process this row
                    if (dv_va_null === -1) dv_va = NW_NR4;
                    if (dv_rd_null === -1) dv_rd = ' ';
                    if (dv_va < NW_CR4) {
                        // value is null
                        // TODO:
                        /*
                          WRITE (tunit) cdate, nullval, nullrd, dv_rmk_cd,
                          *                    dv_type_cd, data_aging_cd
                          */
                    }
                    else {
                        // Pick a rounding precision IF blank
                        if (dv_rd === ' ')
                            if (! nw_va_rget_rd(dv_va, rndary, dv_rd))
                                dv_rd = '9';
                        
                        // convert value to a character string and load it
                        if (rndsup) {
                            /**
                               @todo
                               WRITE (cval,2050) dv_va
                               2050                   FORMAT (E14.7)
                            */
                        }
                        else {
                            if (! nw_va_rrnd_tx(dv_va, dv_rd, cval))
                                cval = '****';
                        }
                        cval = sprintf("%20s", cval);
                        /**
                           @todo
                           WRITE (tunit) cdate, cval, dv_rd, dv_rmk_cd,
                           *                    dv_type_cd, data_aging_cd
                           */
                    }
                    nw_dtinc(odate, 1);
                    // TODO:
                    /*
                      END DO
                      EXEC SQL CLOSE cur_stmt
                      end if
                      end if
                    */
                    if (irc !== 0)
                        return irc;
                    
                    // fill nulls to the end of the period, if the database
                    // retrieval stopped short
                    
                    while (odate <= enwisdt) {
                        // TODO:
                        /*
                          WRITE (tunit) odate, nullval, nullrd, nullrmk, nulltype,
                          *           nullaging
                          */
                        nw_dtinc(odate, 1);
                    }
                    // TODO:
                    // ENDFILE (tunit)

                    // Read the temp file and write the RDB file, filling in data
                    // aging where blank (did it this way because the data aging
                    // routine would COMMIT the loop

                    // TODO:
                    /*
                      REWIND (tunit)
                      30       READ (tunit,END=40) cdate, cval, dv_rd, dv_rmk_cd,
                      *           dv_type_cd, data_aging_cd
                      */
                    if (data_aging_cd === ' ') {
                        if (! nw_db_retr_aging_for_date(dbnum, dd_id, cdate,
                                                        data_aging_cd))
                            return;
                    }

                    var exdate;   // verbose Excel style date "mm/dd/yyyy"

                    if (vflag) {           // build "mm/dd/yyyy"
                        exdate = cdate.substr(4, 5) + '/' + cdate.substr(6, 7) +
                            '/' + cdate.substr(0, 3);
                    }
                    else {                  // copy over "yyyymmdd"
                        exdate = cdate;
                    }

                    if (addkey) {
                        outlin = agyin + '\t' + station + '\t' +
                            pcode.substr(1, 5) + '\t' + stat + '\t' +
                            exdate;
                    }
                    else {
                        outlin = exdate;
                    }

                    if (cval === '**NULL**') {
                        outlin += '\t\t\t\t' + dv_rmk_cd + '\t\t' +
                            dv_type_cd + '\t' + data_aging_cd;
                    }
                    else {
                        outlin += '\t\t' + cval + '\t' + dv_rd + '\t' +
                            dv_rmk_cd + '\t\t' + dv_type_cd + '\t' +
                            data_aging_cd;
                    }

                    /**
                       WRITE (funit,'(20A)') outlin(1:outlen)
                       GOTO 30

                       40      CLOSE (tunit, status = 'DELETE')
                    */
                    funit.write(outlin + '\n', "ascii");
                    callback(null);
                }
            ]); // async.waterfall

            callback(null);
        } // fdvrdbout

        function fuvrdbout(callback) {
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

                    // TODO: "parameter" somehow went MIA in fuvrdbout() formal
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
                   @todo site.request() and site.receive() can be done in
                   parallel with the requesting/receiving of time
                   series descriptions below.
                */
                site.request,
                site.receive,
                function (receivedSite, callback) {
                    waterServicesSite = receivedSite; // set global
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
                           @function Write RDB header to HTTP response.
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
                                parameter,
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
                   @description Write time series data as RDB rows to HTTP
                   response.
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
            );
        } // fuvrdbout

        async.waterfall([
            function (callback) {
                if (docRequest(request.url, "/aq2rdb", response, callback))
                    return;
                callback(null, request.url);
            },
            parseFields,
            rdbOut,
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
               @function Query
                         USGS-parameter-code-to-AQUARIUS-parameter Web
                         service here to obtain AQUARIUS parameter
                         from USGS parameter code.
               @callback
               @param {string} NWIS-RA authorization token.
               @param {function} callback async.waterfall() callback
                                 function.
            */
            function (callback) {
                try {
                    rest.querySecure(
                        options.waterDataHostname,
                        "GET",
                        {"Authorization": "Bearer " + nwisRA.tokenId()},
                        "/service/data/view/parameters/json",
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
            function (callback) {
                callback(
                    null, token, agencyCode, siteNumber,
                    parameter.aquariusParameter, "Instantaneous",
                    "Points"
                );
            },
            getTimeSeriesDescription,
            function (tsd, callback) {
                timeSeriesDescription = tsd; // set variable in outer scope
                callback(null);
            },
            //  get data and output to files
            ifAsync(dataTypeIsDV).then(
                fdvrdbout
            )
            .elseIf(dataTypeIsUV).then(
                fuvrdbout
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
                log(packageName + ".httpdispatcher.onGet(\"/" + packageName +
                    "\", ().async.waterfall([], (error))", error);
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
   @description Service dispatcher.
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
    /**
       @description Set of successfully parsed command line options.
    */
    var options = cli.parse();
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
   @todo NWISRA prototype constructor
*/
var NWISRA = function (hostname, userName, password, log, callback) {
    var authentication;

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
                callback(null);
        }
    );

    this.tokenId = function () {
        return authentication.tokenId;
    }

} // NWISRA

/**
   @description Check for "version" CLI option.
*/
if (options.version === true) {
    getVersion(function (version) {console.log(version);});
}
else {
    /**
       @description Create HTTP server to host the service.
    */
    var server = http.createServer(handleRequest);

    async.parallel([
        function (callback) {
            try {
                nwisRA = new NWISRA(
                    options.waterDataHostname,
                    options.waterDataUserName,
                    options.waterDataPassword, options.log, callback
                );
            }
            catch (error) {
                if (error) {
                    callback(error);
                    return;
                }
            }
            // no callback here; it is called from NWISRA() when complete
        },
        function (callback) {
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
    ],
        function (error) {
            if (error) {
                log(packageName, error);
                return;
            }
            else {
                log(
                    packageName,
                    "Received NWIS-RA authentication token successfully"
                );
                log(
                    packageName,
                    "Received AQUARIUS authentication token successfully"
                );
                /**
                   @description Start listening for requests.
                */ 
                server.listen(options.port, function () {
                    log(
                        packageName,
                        "Server listening on: http://localhost:" +
                        options.port.toString()
                    );
                });
            }
        }
    );
}

/**
   @description Export module's private functions to test harness
                only.
   @see http://engineering.clever.com/2014/07/29/testing-private-functions-in-javascript-modules/
*/
if (process.env.NODE_ENV === "test") {
    module.exports._private = {
        handle: handle,
        jsonParseErrorMessage: jsonParseErrorMessage,
        docRequest: docRequest,
        dvTableRow: dvTableRow,
        nwisVersusIANA: nwisVersusIANA
    };
}
