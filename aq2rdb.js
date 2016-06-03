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

// Node.JS modules
var async = require("async");
var commandLineArgs = require("command-line-args");
var ifAsync = require("if-async");
var fs = require("fs");
var http = require("http");
var httpdispatcher = require("httpdispatcher");
var moment = require("moment-timezone");
var path = require("path");
var sprintf = require("sprintf-js").sprintf;
var url = require("url");

// aq2rdb modules
var adaps = require("./adaps");
var aquaticInformatics = require("./aquaticInformatics");
var rdb = require("./rdb");
var service = require("./service");
var site = require("./site");

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
                    (NWIS-RA) service host.
    */
    {name: "nwisRAHostname", type: String,
     defaultValue: "nwisdata.usgs.gov"},
    /**
       @description USGS NWIS Reporting Application, service account
                    user name.
    */
    {name: "nwisRAUserName", type: String},
    /**
       @description USGS NWIS Reporting Application, service account
                    password.
    */
    {name: "nwisRAPassword", type: String}
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
   @description An incomplete (but reportedly sufficient) mapping of
                NWIS STAT.stat_cd to AQUARIUS ComputationIdentifier.
   @global
   @private
   @type {object}
*/
var computationIdentifier = {
    "00001": "Max",
    "00002": "Min",
    "00003": "Mean"
};

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
    async.waterfall([
        function (callback) {
            aquarius.getRemarkCodes();
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
                            aquarius.remarkCodes,
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
                            new aquaticInformatics.LocationIdentifier(
                                field.LocationIdentifier
                            );
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
                    field.ComputationIdentifier, "Daily"
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
        var parameter, statCd, extendedFilters;
        var uniqueId, during, cflag, vflag, applyRounding, locTzCd;

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
            var timeSeriesDescription;

            async.waterfall([
                function (callback) {
                    if (computationIdentifier[statCd] === undefined) {
                        callback(
                            "Unsupported statistic code \"" + statCd +
                                "\""
                        );
                        return;
                    }

                    callback(
                        null, agencyCode, siteNumber,
                        parameter.aquariusParameter,
                        computationIdentifier[statCd], "Daily"
                    );
                },
                aquarius.getTimeSeriesDescription,
                function (tsd, callback) {                  
                    // save TimeSeriesDescription in outer scope
                    timeSeriesDescription = tsd;

                    var statistic = {code: statCd};

                    try {
                        statistic.name = stat[statCd].name;
                        statistic.description = stat[statCd].description;
                    }
                    catch (error) {
                        callback(
                            "Invalid statistic code \"" + statCd +
                                "\""
                        );
                        return;
                    }

                    // write the header records
                    rdb.header(
                        "NWIS-I DAILY-VALUES", "NO",
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
                                null, "NWIS-I UNIT-VALUES", "NO",
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
                    // Note: "r" field ("rounding suppression")
                    // Boolean truth value is inverted below for
                    // semantic forwards-compatibility with AQUARIUS's
                    // "ApplyRounding" parameter.
                    var parameters = {
                        TimeSeriesUniqueId: uniqueId,
                        ApplyRounding: applyRounding
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
                            var value =
                                (applyRounding === "False") ?
                                point.Value.Numeric :
                                point.Value.Display;
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
                                    value + "\t\t \t\t" +
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
            function (requestURL, callback) {
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

                if ((field.p || field.s) && field.u) {
                    callback(
                        "Specify either \"-p\" and \"s\", or " +
                            "\"-u\", but not both"
                    );
                    return;
                }

                for (var name in field) {
                    if (name.match(/^(a|p|t|s|n|b|e|l|r|u|w|c)$/)) {
                        // aq2rdb fields
                    }
                    else {
                        callback('Unknown field "' + name + '"');
                        return;
                    }
                }

                // "rounding suppression flag"
                applyRounding = (field.r === undefined) ? "True" : "False";

                dataType = field.t.substring(0, 2).toUpperCase();
                agencyCode = field.a;
                siteNumber = field.n;
                parameterCode = field.p;

                // pass parsed field values to next async.waterfall()
                // function
                callback(
                    null, field.w, field.c, false, field.s, field.u,
                    field.b, field.e, field.l, ""
                );
            },
            function rdbOut(
                wyflag, cflag, vflag, instatCd, uniqueId,
                begdat, enddat, locTzCd, titlline, callback
            ) {
                var datatyp, uvtyp, interval, uvtypPrompted = false;

                if (locTzCd === undefined) locTzCd = "LOC";

                // init control argument
                var sopt = "10000000000000000000000000000000".split("");

                // convert agency to 5 characters - default to USGS
                if (agencyCode === undefined)
                    agencyCode = "USGS";
                else
                    agencyCode = agencyCode.substring(0, 5);

                // convert station to 15 characters
                siteNumber = siteNumber.substring(0, 15);

                // further processing depends on data type

                if (dataType === 'DV') {
                    if (instatCd === undefined)
                        sopt[7] = '1';
                    else
                        statCd = instatCd;
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
                    
                    uvtyp = instatCd.charAt(0).toUpperCase();
                    
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
                    null, vflag, dataType, agencyCode,
                    siteNumber, parameterCode, uniqueId, interval,
                    locTzCd
                );
            },
            /**
               @todo It would be quite nice to get rid of this scoping
                     crutch eventually.
            */
            function (
                v, d, a, s, p, u, interval, locTzCd, callback
            ) {
                // save values in outer scope to avoid passing these
                // values through subsequent async.waterfal()
                // functions' scopes where they are not referenced at
                // all
                vflag = v;
                dataType = d;
                locationIdentifier =
                    new aquaticInformatics.LocationIdentifier(a, s);
                parameterCode = p;
                uniqueId = u;
                during = interval;

                callback(null);
            },
            function (callback) {
                /**
                   @todo site.request() and site.receive() can be done
                         in parallel with the requesting/receiving of
                         parameter code mapping below.
                */
                async.waterfall([
                    function (callback) {
                        callback(
                            null, options.waterServicesHostname,
                            locationIdentifier.agencyCode(),
                            locationIdentifier.siteNumber(),
                            options.log
                        );
                    },
                    site.request,
                    site.receive,
                    function (receivedSite, callback) {
                        waterServicesSite = receivedSite; // set global
                        callback(null);
                    }
                ],
                    function (error) {
                        // if the location was not found
                        if (error === 404)
                            callback(
                                "Location " +
                                    locationIdentifier.toString() +
                                    " does not exist"
                            );
                        else
                            callback(error);
                    }
                );
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
                nwisRA.query(
                    {"parameters.PARM_ALIAS_CD": "AQNAME",
                     "parameters.PARM_CD": parameterCode},
                    options.log,
                    callback
                );
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
   @function
   @description Attempt AQUARIUS handshaking to get authentication
                token.
   @private
   @param {function} callback Callback function to call when
   complete.
*/
function initAquarius(callback) {
    try {
        aquarius = new aquaticInformatics.AQUARIUS(
            options.aquariusTokenHostname,
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
} // initAquarius

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
    /**
       @todo Need to handle this error more gracefully:

         aq2rdb: Initialized parameter mapping
         aq2rdb: Received AQUARIUS authentication token successfully
         aq2rdb: Loaded stat.json
         events.js:154
         throw er; // Unhandled 'error' event
         ^

         Error: listen EADDRINUSE :::8081

       This occurs when aq2rdb is already running on the server.
    */
    var server = http.createServer(handleRequest);
    var passwd = new Object();

    async.waterfall([
        function (callback) {
            // if all prerequisite login information is missing on
            // command-line
            if (options.aquariusUserName === undefined &&
                options.aquariusPassword === undefined &&
                options.nwisRAUserName === undefined &&
                options.nwisRAPassword === undefined) {
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
                passwd.nwisRAHostname = options.nwisRAHostname;
                passwd.nwisRAUserName = options.nwisRAUserName;
                passwd.nwisRAPassword = options.nwisRAPassword;
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
                    async.waterfall([
                        function (callback) {
                            nwisRA = new service.NWISRA(
                                passwd.nwisRAHostname,
                                passwd.nwisRAUserName,
                                passwd.nwisRAPassword, options.log,
                                callback
                            );
                            // no callback here; it is called from
                            // NWISRA() when complete
                        }
                    ],
                        function (error) {
                            if (error)
                                callback(error);
                            else
                                callback(
                                    null, "Initialized parameter mapping"
                                );
                        }
                    );
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
        cli: cli,
        docRequest: docRequest,
        dvTableRow: dvTableRow,
        handle: handle,
        jsonParseErrorMessage: jsonParseErrorMessage,
        nwisVersusIANA: nwisVersusIANA,
        options: options
    };
}
