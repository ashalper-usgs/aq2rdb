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
var commandLineArgs = require("command-line-args");
var fs = require("fs");
var http = require("http");
var httpdispatcher = require("httpdispatcher");
var moment = require("moment-timezone");
var path = require("path");
var url = require("url");

// aq2rdb modules
var adaps = require("./adaps");
var aquaticInformatics = require("./aquaticInformatics");
var rdb = require("./rdb");
var site = require("./site");
var usgs = require("./usgs");

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
     defaultValue: "testqa.owicloud.org"},
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
        statusMessage = rdb.comment(
            packageName + ': Connection error; a common cause of ' +
                'this is GetAQToken being unreachable'
        );
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
   @param {string} name Route name.
*/
function docRequest(name) {
    return new Promise(function (resolve, reject) {
        // read and serve the documentation page
        fs.readFile(
            "doc/" + path.basename(name) + ".html",
            function (error, html) {
                if (error)
                    reject(error);
                else
                    resolve(html);
            }
        );
    });
} // docRequest

/**
   @function
   @description Create RDB, DV table row.
   @private
   @param {string} timestamp AQUARIUS timestamp string.
   @param {object} value Time series daily value object.
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
    for (var i = 0, l = qualifiers.length; i < l; i++) {
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
            break;
        }
    }
    row += "\t ";

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
   @description Parse version number from "package.json" and return in
                a promise.
   @private
 */
function getVersion() {
    return new Promise(function (resolve, reject) {
        fs.readFile("package.json", function (error, json) {
            if (error) {
                reject(error);
                return;
            }
            
            var pkg;
            try {
                pkg = JSON.parse(json);
            }
            catch (error) {
                return;
            }

            resolve(pkg.version);
        });
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
   @param {object} interval Interval object.
   @param {string} tzCode Time zone abbreviation.
   @param {string} fromTheBeginningOfTimeToken Token used in NWIS
          ADAPS to represent the "from the beginning of time" search
          condition.
   @param {string} toTheEndOfTimeToken Token used in NWIS ADAPS to
          represent the "to the end of time" search condition.
   @see http://momentjs.com/timezone/docs/#/using-timezones/
*/
function appendIntervalSearchCondition(
    parameters, interval, tzCode,
    fromTheBeginningOfTimeToken, toTheEndOfTimeToken
) {
    // if "from" interval boundary is not "from the beginning of time"
    if (interval.from !== fromTheBeginningOfTimeToken) {
        try {
            parameters["QueryFrom"] =
                moment.tz(interval.from, "YYYYMMDDHHmmss", tzCode).format();
        }
        catch (error) {
            throw error;
            return;
        }
    }

    // if "to" interval boundary is not "to the end of time"
    if (interval.to !== toTheEndOfTimeToken) {
        try {
            parameters["QueryTo"] =
                moment.tz(interval.to, "YYYYMMDDHHmmss", tzCode).format();
        }
        catch (error) {
            throw error;
            return;
        }
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
*/
function dvTableBody(
    timeSeriesUniqueId, queryFrom, queryTo, tzName, response
) {
    // load mapping of NWIS remark codes to AQUARIUS qualifiers
    return aquarius.getRemarkCodes()
        .then(() => {
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
                    throw error;
                    return;
                }
                parameters["QueryFrom"] = f;
            }

            if (queryTo !== "99999999") {
                try {
                    t = moment.tz(queryTo, tzName).format("YYYY-MM-DD");
                }
                catch (error) {
                    throw error;
                    return;
                }
                parameters["QueryTo"] = t;
            }

            return aquarius.getTimeSeriesCorrectedData(parameters);
        })
        .then((messageBody) =>
	      aquarius.parseTimeSeriesDataServiceResponse(messageBody))
        .then((timeSeriesDataServiceResponse) => {
            var p = timeSeriesDataServiceResponse.Points;

            for (var i = 0, l = p.length; i < l; i++) {
                response.write(
                    dvTableRow(
                        p[i].Timestamp,
                        p[i].Value,
                        timeSeriesDataServiceResponse.Qualifiers,
                        aquarius.remarkCodes,
          timeSeriesDataServiceResponse.Approvals[0].LevelDescription.charAt(0)
                    ),
                    "ascii"
                );
            }
        })
        .catch((error) => {throw error;});
} // dvTableBody

function uvTableBody (
    applyRounding, tzCode, localTimeFlag, qaCode, points, response
) {
    return new Promise(function (resolve, reject) {
        for (var i = 0, l = points.length; i < l; i++) {
            var value, zone;

            if (applyRounding === "True")
                value = points[i].Value.Display;
            else if (applyRounding === "False")
                value = points[i].Value.Numeric.toString();
            else
                throw 'Invalid applyRounding value "' +
                      applyRounding +
                      '" passed to aq2rdb.uvTableBody()';

            try {
                zone = tzName[tzCode][localTimeFlag];
            }
            catch (error) {
                reject(
                    "Could not look up IANA time zone " +
                        "name for site time zone spec. " +
                        "(" + tzCode + "," + localTimeFlag + ")"
                );
                return;
            }

            // reference AQUARIUS timestamp to site's (NWIS) time zone
            // spec.
            var m = moment.tz(points[i].Timestamp, zone);

            response.write(
                m.format("YYYYMMDD") + '\t' + m.format("HHmmss") +
                    '\t' + tzCode + '\t' + value + "\t\t \t\t" +
                    qaCode + '\n'
            );
        }
        resolve();
    });
} // uvTableBody

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

        // if this is a documentation page request
        if (request.url === "/" + packageName + "/GetDVTable")
            // serve the documentation page
            docRequest("GetDVTable").then((html) => {
                response.writeHeader(200, {"Content-Type": "text/html"});  
                response.end(html);
            });
        else {
            var p = new Promise(function (resolve, reject) {
                try {
                    field = url.parse(request.url, true).query;
                }
                catch (error) {
                    reject(error);
                    return;
                }

                for (var name in field) {
                    if (name === "LocationIdentifier") {
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
                    else if (name !== "") {
                        reject("Unknown field \"" + name + "\"");
                        return;
                    }
                }

                if (locationIdentifier === undefined) {
                    reject('Required field "LocationIdentifier" not found');
                    return;
                }

                if (field.Parameter === undefined) {
                    reject('Required field "Parameter" not found');
                    return;
                }

                resolve(
                    locationIdentifier.agencyCode(),
                    locationIdentifier.siteNumber(), field.Parameter,
                    field.ComputationIdentifier, "Daily"
                );
            });

            p.then((agencyCode, siteNumber, parameter,
                    computationIdentifier, computationPeriodIdentifier) => {
                        return aquarius.getTimeSeriesDescription(
                            agencyCode, siteNumber, parameter,
                            computationIdentifier,
                            computationPeriodIdentifier
                        );
                    })
                .then((tsd) => {
                    /** @todo Bad. Try to factor out. */
                    timeSeriesDescription = tsd;

                    return site.request(
                        options.waterServicesHostname,
                        locationIdentifier.agencyCode(),
                        locationIdentifier.siteNumber(), options.log
                    );
                })
                .then((messageBody) => {
                    return site.receive(messageBody);
                })
                .then((site) => {
                    var start, end;

                    if (field.QueryFrom !== undefined) {
                        start = aq2rdb.toNWISDateFormat(field.QueryFrom);
                    }

                    if (field.QueryTo !== undefined) {
                        end = aq2rdb.toNWISDateFormat(field.QueryTo);
                    }

                    response.writeHead(
                        200, {"Content-Type": "text/plain"}
                    );

                    response.write(
                        rdb.header(
                            "NWIS-I DAILY-VALUES", "YES", site,
                            subLocationIdentifer, parameter,
                            {code: "", name: "", description: ""},
                            {start: start, end: end}
                        ) + 'DATE\tTIME\tVALUE\tREMARK\tFLAGS\tTYPE\tQA\n' +
                            '8D\t6S\t16N\t1S\t32S\t1S\t1S\n',
                        "ascii"
                    );
                })
                .then(() => dvTableBody(
                    timeSeriesDescription.UniqueId,
                    field.QueryFrom, field.QueryTo,
             tzName[waterServicesSite.tzCode][waterServicesSite.localTimeFlag],
                    response
                ))
                .then(() => response.end())
                .catch((error) => {
                    log(packageName + ".GetDVTable()", error);
                    throw error;
                });
        }
    }
); // GetDVTable

/**
   @description GetUVTable endpoint service request handler.
*/
httpdispatcher.onGet(
    "/" + packageName + "/GetUVTable",
    /**
       @callback
    */
    function (request, response) {
        var field;

        // if this is a documentation page request
        if (request.url === "/" + packageName + "/GetUVTable") {
            docRequest("GetUVTable").then((html) => {
                // serve the documentation page
                response.writeHeader(200, {"Content-Type": "text/html"});  
                response.end(html);
            })
                .catch((error) => {
                    response.end("# " + packageName + ": " + error);
                    log(packageName, error);
                });
            return;
        }

        // parse REST query
        try {
            field = url.parse(request.url, true).query;
        }
        catch (error) {
            throw error;
            return;
        }

        // if any required fields are omitted
        if (! ("TimeSeriesIdentifier" in field ||
               "LocationIdentifier" in field && "Parameter" in field &&
               "ComputationIdentifier" in field)) {
            // respond with error
            response.writeHeader(400, {"Content-Type": "text/plain"});  
            response.end(
                "# " + packageName +
                    ": TimeSeriesIdentifier, or LocationIdentifer " +
                    "and Parameter and ComputationIdentifier fields " +
                    "must be present"
            );
            return;
        }

        // if we have redundant information
        if ("TimeSeriesIdentifier" in field &&
            "LocationIdentifier" in field && "Parameter" in field &&
            "ComputationIdentifier" in field) {
            // respond with error
            response.writeHeader(400, {"Content-Type": "text/plain"});
            response.end(
                "# " + packageName +
                    ": Either TimeSeriesIdentifier, or LocationIdentifer " +
                    "and Parameter and ComputationIdentifier fields " +
                    "must be present; not both"
            );
            return;
        }

        var timeSeriesIdentifier = field.TimeSeriesIdentifier;

        // parse LocationIdentifier string from TimeSeriesIdentifier
        var locationIdentifierString = timeSeriesIdentifier.split('@')[1];

        // if LocationIdentifier value is not usable
        if (locationIdentifierString === undefined ||
            locationIdentifierString === "") {
            // end with error message
            response.writeHeader(400, {"Content-Type": "text/plain"});
            response.end(
                "# " + packageName + ": Could not find a " +
                    "LocationIdentifier in TimeSeriesIdentifier \"" +
                    field.TimeSeriesIdentifier + "\""
            );
            return;
        }

        // check syntax of QueryFrom value
        var queryFrom;
        if ("QueryFrom" in field) {
            try {
                var d = new Date(field.QueryFrom);
            }
            catch (error) {
                throw error;
                return;
            }
            queryFrom = field.QueryFrom;
        }

        // check syntax of QueryTo value
        var queryTo;
        if ("QueryTo" in field) {
            try {
                var d = new Date(field.QueryTo);
            }
            catch (error) {
                throw error;
                return;
            }
            queryTo = field.QueryTo;
        }

        var applyRounding =
            ("ApplyRounding" in field) ? field.ApplyRounding : "False";

        var timeSeriesDescription;
        var site = new usgs.Site(locationIdentifierString);
        var siteLoad = site.load(options.waterServicesHostname);
        var getTimeSeriesDescriptionList =
            aquarius.getTimeSeriesDescriptionList({
                LocationIdentifier: locationIdentifierString
            });

        Promise.all([
            siteLoad,
            getTimeSeriesDescriptionList
        ]).then(function (prerequisite) {
            var timeSeriesDescriptionListServiceResponse =
                JSON.parse(prerequisite[1]);
            var timeSeriesDescriptions =
                timeSeriesDescriptionListServiceResponse.TimeSeriesDescriptions;
            var projection = new Array();

            for (var i = 0, l = timeSeriesDescriptions.length; i < l; i++) {
                if (timeSeriesIdentifier ===
                    timeSeriesDescriptions[i].Identifier)
                    // save matched record
                    projection.push(timeSeriesDescriptions[i]);
            }

            // check arity
            if (projection.length === 0)
                throw 'No TimeSeriesDescription found matching ' +
                'TimeSeriesIdentifier "' +
                timeSeriesIdentifier + '"';
            else if (1 < projection.length)
                throw 'More than one TimeSeriesDescription matching ' +
                'TimeSeriesIdentifier "' +
                timeSeriesIdentifier + '" found'
            else
                timeSeriesDescription = projection[0];

            var header = "# //UNITED STATES GEOLOGICAL SURVEY " +
                "      http://water.usgs.gov/\n" +
                "# //NATIONAL WATER INFORMATION SYSTEM " +
                "    http://water.usgs.gov/data.html\n" +
                "# //DATA ARE PROVISIONAL AND SUBJECT TO " +
                "CHANGE UNTIL PUBLISHED BY USGS\n" +
                "# //RETRIEVED: " +
                moment().format("YYYY-MM-DD HH:mm:ss") + '\n' +
                '# //FILE TYPE="NWIS-I UNIT-VALUES" ' + 'EDITABLE=NO\n';

            /**
               @todo this part might be a bit
                     weird/inviting-a-race-condition, because
                     Site.load().then() is getting called to populate
                     site properties within usgs.Site, when it seems
                     like we should be dealing with it via
                     prerequisite[0]?
            */
            header +=
                sprintf(
        '# //STATION AGENCY="%-5s" NUMBER="%-15s" TIME_ZONE="%s" DST_FLAG=%s\n',
                    site.agencyCode, site.number, site.tzCode,
                    site.localTimeFlag
                ) + '# //STATION NAME="' + site.name + '"\n' +
                '# //TimeSeriesIdentifier="' + timeSeriesIdentifier + '"\n';

            if (timeSeriesDescription.subLocationIdentifer !== undefined) {
                header += '# //SUBLOCATION ID="' +
                    subLocationIdentifer + '"\n';
            }

            header += '# //RANGE START="';
            if (queryFrom === undefined) {
                // hacky, legacy NWIS syntax for representing "from
                // the beginning of time" predicate
                header += "00000000000000";
            }
            else {
                header += aq2rdb.toNWISDatetimeFormat(queryFrom);
            }
            header += '"';
    
            header += ' END="';
            if (queryTo === undefined) {
                // hacky, legacy NWIS syntax for representing "until
                // the end of time" predicate
                header += "99999999999999";
            }
            else {
                header += aq2rdb.toNWISDatetimeFormat(queryTo);
            }
            header += '"\n';

            response.write(header, "ascii");

            // RDB heading (a different thing than a header)
            response.write(
                header +
                    "DATE\tTIME\tTZCD\tVALUE\tPRECISION\tREMARK\tFLAGS\tQA\n" +
                    "8D\t6S\t6S\t16N\t1S\t1S\t32S\t1S\n",
                "ascii"
            );
        }, function (error) {   // error handler for Promise.all() above
            response.end("# " + error, "ascii");
        }).then(() => {
            aquarius.getTimeSeriesCorrectedData(
                {TimeSeriesUniqueId: timeSeriesDescription.UniqueId,
                 ApplyRounding: applyRounding,
                 QueryFrom: queryFrom, QueryTo: queryTo}
            )
                .then((timeSeriesDataServiceResponse) => {
                    // if there are no unit values
                    if (timeSeriesDataServiceResponse.Points === undefined) {
                        // stop
                        response.end();
                        return;
                    }

                    uvTableBody(
                        /**
                           @todo What should applyRounding be in this
                                 context?
                        */
                        "True",
                        site.tzCode,
                        site.localTimeFlag,
                        // QA code ("QA" RDB column): might not be
                        // backwards-compatible with nwts2rdb
         timeSeriesDataServiceResponse.Approvals[0].LevelDescription.charAt(0),
                        timeSeriesDataServiceResponse.Points,
                        response
                    )
                    .then(() => {response.end();})
                    .catch((error) =>
                           {response.end(rdb.comment(error), "ascii");});
                });
        })
            .catch((error) => {response.end(rdb.comment(error), "ascii");});
    }
); // GetUVTable

/**
   @function
   @description "aq2rdb/" path subroutine to write daily values to HTTP
                response.
*/
function dailyValues(site, parameter, statCd, interval, response) {
    var timeSeriesDescription, statistic;

    if (computationIdentifier[statCd] === undefined) {
        throw 'Unsupported statistic code "' + statCd + '"';
        return;
    }

    return aquarius.getTimeSeriesDescription(
        site.agencyCode, site.number, parameter.aquariusParameter,
        computationIdentifier[statCd], "Daily"
    )
        .then((tsd) => {
            timeSeriesDescription = tsd;
            statistic = {code: statCd};

            try {
                statistic.name = stat[statCd].name;
                statistic.description = stat[statCd].description;
            }
            catch (error) {
                throw 'Invalid statistic code "' + statCd + '"';
                return;
            }

            return rdb.header(
                "NWIS-I DAILY-VALUES", "NO",
                site,
                timeSeriesDescription.SubLocationIdentifer,
                parameter, statistic,
                /**
                   @todo Hard-coded object here is likely not
                   correct under all circumstances.
                */
                {name: "FINAL",
                 description: "EDITED AND COMPUTED DAILY VALUES"},
                {start: interval.from, end: interval.to}
            );
        })
        .then((header) => {
            // write RDB column headings
            response.write(
                header +
                    "DATE\tTIME\tVALUE\tPRECISION\tREMARK\tFLAGS\tTYPE\tQA\n" +
                    "8D\t6S\t16N\t1S\t1S\t32S\t1S\t1S\n",
                "ascii"
            );
        })
        .then(() => dvTableBody(
            timeSeriesDescription.UniqueId,
            interval.from, interval.to,
            tzName[site.tzCode][site.localTimeFlag],
            response
        ))
        .then(() => response.end())
        .catch((error) => {
            log(packageName + ".GetDVTable()", error);
            throw error;
        });
} // dailyValues

function unitValues(site, parameter, interval, applyRounding, response) {
    var uniqueId;

    return aquarius.getTimeSeriesDescription(
        site.agencyCode, site.number, parameter.aquariusParameter,
        "Instantaneous", "Points"
    )
        .then((timeSeriesDescription) => {
            // save in outer scope because it gets referenced in a
            // then() call below
            uniqueId = timeSeriesDescription.UniqueId;

            response.write(
                rdb.header(
                    "NWIS-I UNIT-VALUES", "NO",
                    site,
                    timeSeriesDescription.SubLocationIdentifer,
                    /**
                       @todo need to find out what to pass in for
                       "statistic" parameter when doing UVs below.
                    */
                    parameter, undefined,
                    /**
                       @todo this is pragmatically hard-coded now
                    */
                    {code: 'C', name: "COMPUTED"},
                    {start: interval.from, end: interval.to}
                )
            );
        })
        .then(() => response.write(
            "DATE\tTIME\tTZCD\tVALUE\tPRECISION\tREMARK\tFLAGS\tQA\n" +
                "8D\t6S\t6S\t16N\t1S\t1S\t32S\t1S\n",
            "ascii"
        ))
        .then(() => aquarius.getTimeSeriesCorrectedData(
            appendIntervalSearchCondition(
                {TimeSeriesUniqueId: uniqueId,
                 ApplyRounding: applyRounding},
                interval, site.tzCode,
                "00000000000000", "99999999999999"
            )
        ))
        .then((messageBody) => {
            var timeSeriesDataServiceResponse =
                aquarius.parseTimeSeriesDataServiceResponse(messageBody);

            return timeSeriesDataServiceResponse;
        })
        .then((timeSeriesDataServiceResponse) => uvTableBody(
            applyRounding,
            site.tzCode,
            site.localTimeFlag,
            // QA code ("QA" RDB column): might not be
            // backwards-compatible with nwts2rdb
         timeSeriesDataServiceResponse.Approvals[0].LevelDescription.charAt(0),
            timeSeriesDataServiceResponse.Points,
            response
        ))
        .then(() => response.end())
        .catch((error) => {
            log(packageName + ".unitValues()", error);
            throw error;
        });
} // unitValues

function query(requestURL, response) {
    return new Promise(function (resolve, reject) {
        var field;

        try {
            field = url.parse(requestURL, true).query;
        }
        catch (error) {
            reject(error);
            return;
        }

        // if any mandatory fields are missing
        if (field.t === undefined || field.n === undefined ||
            field.b === undefined || field.e === undefined ||
            field.s === undefined || field.p === undefined) {
            // terminate response with an error
            reject(
                "All of \"t\", \"n\", \"b\", \"e\", \"s\" and \"p\" " +
                    "fields must be present" 
            );
            return;
        }

        if ((field.p || field.s) && field.u) {
            reject(
                "Specify either \"-p\" and \"s\", or " +
                    "\"-u\", but not both"
            );
            return;
        }

        for (var name in field) {
            if (name.match(/^(a|p|t|s|n|b|e|l|r|u|w|c)$/)) {
                // aq2rdb fields
            }
            else if (name !== "") {
                reject("Unknown field \"" + name + "\"");
                return;
            }
        }

        var dataType = field.t.substring(0, 2).toUpperCase();
        var agencyCode = ("a" in field) ? field.a.substring(0, 5) : "USGS";
        // convert station to 15 characters
        var siteNumber = field.n.substring(0, 15);
        var parameterCode = field.p;

        var begdat = field.b;
        var enddat = field.e;

        // "rounding suppression flag"; note that AQUARIUS
        // ApplyRounding semantics require effectively inverting the
        // truth value here
        var applyRounding = ("r" in field) ? "False" : "True";

        var wyflag = field.w;
        var cflag = field.c;
        var vflag = false;

        var locTzCd = ("l" in field) ? field.l : "LOC";
        var titlline = "";

        var statCd = field.s;

        if (dataType === 'DV' || dataType === 'DC' ||
            dataType === 'SV' || dataType === 'PK') {
            // convert dates to 8 characters
            if (begdat !== undefined && enddat !== undefined)
                interval = new adaps.IntervalDay(begdat, enddat, wyflag);
        }

        var uvType, interval;

        if (dataType === 'UV') {
            
            uvType = field.s.charAt(0).toUpperCase();
            
            if (! (uvType === 'C' || uvType === 'E' ||
                   uvType === 'M' || uvType === 'N' ||
                   uvType === 'R' || uvType === 'S')) {
                // Position of this is an artifact of the
                // nwts2rdb legacy code: it might need to be
                // moved earlier in HTTP query parameter
                // validation code.
                reject('UV type code must be ' +
                       '"M", "N", "E", "R", "S", or "C"');
                return;
            }

            // convert date/times to 14 characters
            if (begdat !== undefined && enddat !== undefined) {
                interval =
                    new adaps.IntervalSecond(begdat, enddat, wyflag);
            }

        }

        var locationIdentifier =
            new aquaticInformatics.LocationIdentifier(agencyCode, siteNumber);

        var parameter;
        nwisRA.query(
            {"parameters.PARM_ALIAS_CD": "AQNAME",
             "parameters.PARM_CD": parameterCode},
            options.log
        )
            .then((messageBody) => {
                var parameters;

                try {
                    parameters = JSON.parse(messageBody);
                }
                catch (error) {
                    throw error;
                    return;
                }

                // load fields we need into something more coherent
                parameter = {
                    code: parameters.records[0].PARM_CD,
                    name: parameters.records[0].PARM_NM,
                    description: parameters.records[0].PARM_DS,
                    aquariusParameter: parameters.records[0].PARM_ALIAS_NM
                };
            })
            .then(() => site.request(
                options.waterServicesHostname,
                locationIdentifier.agencyCode(),
                locationIdentifier.siteNumber(),
                options.log
            ))
            .then((messageBody) => site.receive(messageBody))
            .catch((error) => {
                if (error === 404)
                    throw "Location " +
                        locationIdentifier.toString() +
                        " does not exist";
                else
                    throw error;
            })
            .then((site) => {
                if (dataType === "DV")
                    dailyValues(
                        site, parameter, statCd, interval, response
                    );
                else if (dataType === "UV")
                    unitValues(
                        site, parameter, interval, applyRounding,
                        response
                    );
                else
                    throw 'Unknown data type "' + dataType + '"';
            });
    });
} // query

/**
   @description aq2rdb endpoint, service request handler.
*/
httpdispatcher.onGet(
    '/' + packageName,
    /**
       @callback
    */
    function (request, response) {
        if (request.url === "/aq2rdb")
            // serve the documentation page
            docRequest("aq2rdb").then((html) => {
                response.writeHeader(200, {"Content-Type": "text/html"});  
                response.end(html);
            });
        else
            query(request.url, response)
            .then(() => response.end())
            .catch((error) => response.end(
                "# " + packageName + ": " + error + '\n',
                "ascii"));
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
        getVersion().then((version) => {
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
        }).catch((error) => {throw error;});
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

    var p = new Promise((resolve, reject) => {
        var passwd = new Object();

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
                        reject(error);
                        return;
                    }

                    try {
                        passwd = JSON.parse(json);
                    }
                    catch (error) {
                        reject(error);
                        return;
                    }
                    resolve(passwd);
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
            resolve(passwd);
        }
    });

    p.then((passwd) => {
        nwisRA = new usgs.NWISRA(
            passwd.nwisRAHostname, passwd.nwisRAUserName,
            passwd.nwisRAPassword, options.log
        );
        aquarius = new aquaticInformatics.AQUARIUS(
            options.aquariusTokenHostname,
            options.aquariusHostname,
            options.aquariusUserName,
            options.aquariusPassword
        );
        var loadStat = new Promise((resolve, reject) => {
            fs.readFile("stat.json", function (error, json) {
                if (error) {
                    reject(error);
                    return;
                }

                try {
                    stat = JSON.parse(json);
                }
                catch (error) {
                    reject(error);
                    return;
                }

                resolve();
            });
        });

        return Promise.all([
            nwisRA.authenticate()
                .then(() => {
                    log(packageName, "Initialized parameter mapping");
                }),
            aquarius.authenticate()
                .then((message) => {log(packageName, message);}),
            loadStat
                .then(() => {log(packageName, "Loaded stat.json");})
        ])
            .catch((error) => {log(packageName, error);});
    })
        .then(() => {
            /** @description Start listening for requests. */ 
            server.listen(options.port, function () {
                log(packageName,
                    "Server listening on: http://localhost:" +
                    options.port.toString());
                // Reconstruct the "aquarius" object every 59
                // minutes to renew lease on authentication
                // token. See
                // https://nodejs.org/api/timers.html#timers_setinterval_callback_delay_arg
                setInterval(
                    function () {
                        aquarius = new aquaticInformatics.AQUARIUS(
                            options.aquariusTokenHostname,
                            options.aquariusHostname,
                            options.aquariusUserName,
                            options.aquariusPassword
                        );
                    },
                    // call above function every 59 minutes:
                    59 * 60 * 1000
                );
            });
        })
        .catch((error) => {
            if (error.code === "ENOENT")
                log(packageName,
                    "No command-line credentials specified, and no " +
                    "password file found at " +
                    error.path);
            else
                log(packageName, error);
        });
}

/**
   @description Export module's private functions to test harness
                only.
   @see http://engineering.clever.com/2014/07/29/testing-private-functions-in-javascript-modules/
*/
if (process.env.NODE_ENV === "test") {
    module.exports._private = {
        appendIntervalSearchCondition: appendIntervalSearchCondition,
        cli: cli,
        docRequest: docRequest,
        dvTableRow: dvTableRow,
        handle: handle,
        jsonParseErrorMessage: jsonParseErrorMessage,
        nwisVersusIANA: nwisVersusIANA,
        options: options
    };
}
