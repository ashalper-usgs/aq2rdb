/**
 * @fileOverview A Web service to map AQUARIUS, time series data
 *               requests to USGS-variant RDB files.
 *
 * @author <a href="mailto:ashalper@usgs.gov">Andrew Halper</a>
 *
 * @see <a href="https://sites.google.com/a/usgs.gov/nwis_integrator/data_retrieval/cli/aqts2rdb">aqts2rdb</a>.
 */

'use strict';
var http = require('http');
var httpdispatcher = require('httpdispatcher');
var querystring = require('querystring');

/**
   @description The aq2rdb Web service name.
*/
var PACKAGE_NAME = 'aq2rdb';

/**
   @description The port the aq2rdb service listens on.
*/
var PORT = 8081;

/**
   @description AQUARIUS host.
*/
var AQUARIUS_HOSTNAME = 'nwists.usgs.gov';

/**
   @description AQUARIUS Web services path prefix.
*/
var AQUARIUS_PREFIX = '/AQUARIUS/Publish/V2/';

/**
   @description Consolidated error message writer. Writes message in
                a single-line, RDB comment.
*/ 
function rdbMessage(response, statusCode, message) {
    var statusMessage = '# ' + PACKAGE_NAME + ': ' + message;

    response.writeHead(statusCode, statusMessage,
                       {'Content-Length': statusMessage.length,
                        'Content-Type': 'text/plain'});
    response.end(statusMessage);
}

/**
   @description Convert an ISO 8601 date string to (a specific
                instance of) an RFC3339 date (for our purposes, the
                instance of which doesn't happen to have a specific
                name).
*/
function rfc3339(isoString) {
    return isoString.replace('T', ' ').replace(/\.\d*/, '');
}

/**
   @description Primitive logging function for debugging purposes.
*/
function log(message) {
    console.log(PACKAGE_NAME + ': ' + message);
}

/**
   @description Create a valid HTTP query field/value pair substring.
*/ 
function bind(field, value) {
    if (value === undefined) {
        return '';
    }
    return '&' + field + '=' + value;
}

/**
   @description Error messager for JSON parse errors.
*/ 
function jsonParseErrorMessage(response, message) {
    rdbMessage(
        response, 502, 
        'While trying to parse a JSON response from ' +
            'AQUARIUS: ' + message
    );
}

/**
   @description Query object prototype.
*/
var Query = function (aq2rdbRequest, aq2rdbResponse) {
    // parse HTTP query
    var arg = querystring.parse(aq2rdbRequest.url);
    // aqToken object, created at bottom of function
    var aqToken;

    if (arg.userName.length === 0) {
        throw 'Required parameter \"userName\" not found';
    }

    if (arg.password.length === 0) {
        throw 'Required parameter \"password\" not found';
    }

    // TODO: this might need to be factored-out eventually
    if (arg.u !== undefined &&
        (arg.a !== undefined || arg.n !== undefined ||
         arg.t !== undefined || arg.s !== undefined ||
         arg.d !== undefined || arg.r !== undefined ||
         arg.p !== undefined)
       ) {
        throw 'If \"u\" is specified, \"a\", \"n\", \"t\", \"s\", ' +
            '\"d\", \"r\", and \"p\" must be omitted';
    }

    this.request = aq2rdbRequest;
    this.response = aq2rdbResponse;

    // TODO: this might have multiple entry points and need to be
    // declared in global scope eventually.
    /**
       @description Figure out what the aq2rdb request is, then call
                    the necessary AQUARIUS API services to accomplish
                    it.
    */
    function dispatch() {
        var parameters = {
            // Please pardon our weirdness while we transition from
            // "Pseudoclassical" constructors to "Functional"
            // constructors.
            token: aqToken.toString(),
            environment: 'production'
        };

        var dataType, ddID;

        // get HTTP query arguments
        for (var opt in arg) {
            switch (opt) {
            case 'z':
                // -z now indicates environment, not database number. In
                // AQUARIUS Era, logical database numbers have been
                // superseded by named time-series environments. The
                // default is 'production', and will work fine unless you
                // know otherwise.
                parameters.environment = arg[opt];
                break;
            case 'a':
                parameters.agencyCode = arg[opt];
                break;
            case 'b':
                parameters.queryFrom = arg[opt];
                break;
            case 'n':
                // AQUARIUS seems to do ill-advised, implicit things with
                // site PK
                parameters.locationIdentifier =
                    arg.a === undefined ? arg[opt] : arg[opt] + '-' + arg.a;
                break;
            case 'u':
                // -u is a new option for specifying a time-series
                // identifier, and is preferred over -d whenever possible
                parameters.timeSeriesIdentifier = arg[opt];
                break;
            case 'd':
                // -d data descriptor number is supported but is
                // deprecated. This option will work for time series that
                // were migrated from ADAPS. This will *not work for new
                // time series* created in AQUARIUS (relies on ADAPS_DD
                // time-series extended attribute). We prefer you use -u
                // whenever possible.
                ddID = arg[opt];
                // TODO:
                /*
                  {
                  "Name": "ADAPS_DD",
                  "Type": "Decimal",
                  "Value": 1
                  },

                  parameters.extendedFilters = 
                */
                break;
            case 'r':
                parameters.applyRounding = false;
                break;
            case 'c':
                // For [data] type "dv", Output COMPUTED daily values
                // only. For other types except pseudo-UV retrievals,
                // combine date and time in a single column.
                parameters.computed = true;
                break;
            case 't':
                var dataType;
                try {
                    dataType = new DataType(arg[opt].toUpperCase());
                }
                catch (error) {
                    rdbMessage(this.response, 400, error);
                    return;
                }
                parameters.computationPeriodIdentifier =
                    dataType.toComputationPeriodIdentifier();
                break;
            case 'l':
                parameters.timeOffset = arg[opt];
                break;
            case 'e':
                parameters.queryTo = arg[opt];
                break;
            }
        }

        // begin date validation
        if (parameters.queryFrom !== undefined) {
            // if (AQUARIUS) queryFrom (aq2rdb "b") field is not a valid
            // ISO date string
            if (isNaN(Date.parse(parameters.queryFrom))) {
                rdbMessage(
                    this.response, 400,
                    PACKAGE_NAME +
                        ': If \"b\" is specified, a valid 14-digit ISO ' +
                        'date must be provided'
                );
                return;
            }
        }           

        // Arguments -n, -d, -t, -i, and -y if -t is "MEAS" must
        // be present as the prompting for missing arguments uses
        // ADAPS subroutines that write the prompts to stdout
        // (they're ports form Prime, remember?  so the RDB output
        // has to go to a file, otherwise the prompts would be
        // mixed in with the RDB output which would make things
        // difficult for a pipeline. -z, and -a will default, and
        // -m is ignored.
        if (dataType === 'MEAS' &&
            (parameters.locationIdentifier === undefined ||
             ddID === undefined ||
             parameters.transportCode === undefined)) {
            rdbMessage(
                this.response, 400,
                '\"n\" and \"d\" fields ' +
                    'must be present when \"t\" is \"MEAS\"'
            );
            return;
        }

        /**
           @description Handle response from GetTimeSeriesDescriptionList.
        */
        function callback(response) {
            var messageBody = '';

            // accumulate response
            response.on(
                'data',
                function (chunk) {
                    messageBody += chunk;
                });

            response.on('end', function () {
                var timeSeriesDescriptionServiceRequest;

                try {
                    timeSeriesDescriptionServiceRequest =
                        JSON.parse(messageBody);
                }
                catch (e) {
                    jsonParseErrorMessage(aq2rdbResponse, e.message);
                    return;         // go no further
                }

                // if the GetTimeSeriesDescriptionList query returned no
                // time series descriptions
                if (timeSeriesDescriptionServiceRequest.TimeSeriesDescriptions
                    === undefined) {
                    // there's nothing more we can do
                    rdbMessage(
                        aq2rdbResponse, 200,
                        'The query found no time series ' +
                            'descriptions in AQUARIUS'
                    );
                    return;
                }

                var dvTable =
                    new DVTable(
                     parameters,
                     timeSeriesDescriptionServiceRequest.TimeSeriesDescriptions
                    );

                // get the DVs from AQUARIUS and respond with the RDB file
                dvTable.toRDB(aq2rdbResponse);
            });
        } // callback

        // if time-series identifier is not present, and location
        // identifier is present
        if (parameters.timeSeriesIdentifier !== undefined) {
            // The object this constructor creates is presently not
            // needed [with everything useful subsequently happening
            // in the context of callback()], but that could change in
            // the future, so the constructor's return value would
            // need to be saved, and possibly declared at an outer
            // scope.
            timeSeriesDescriptionListClass({
                token: aqToken.toString(),
                timeSeriesIdentifier: parameters.timeSeriesIdentifier,
                callback: callback
            });
        }
    } // dispatch

    try {
        aqToken = aqTokenClass({
            userName: arg.userName,
            password: arg.password,
            callback: dispatch
        });
    }
    catch (error) {
        throw error;
    }
} // Query

var DataType = function (text) {
    // data type ("t") parameter domain validation
    switch (text) {
    case 'MS':
        throw 'Pseudo-time series (e.g., gage inspections) are not supported';
        break;
    case 'VT':
        throw 'Sensor inspections and readings are not supported';
        break;
    case 'PK':
        throw 'Peak-flow data are not supported';
        break;
    case 'DC':
        throw 'Data corrections are not supported';
        break;
    case 'SV':
        throw 'Quantitative site-visit data are not supported';
        break;
    case 'WL':
        throw 'Discrete groundwater-levels data are not supported';
        break;
    case 'QW':
        throw 'Discrete water quality data are not supported';
        break;
    // these are the only valid "t" parameter values right now
    case 'DV':
    case 'UV':
        break;
    default:
        throw 'Unknown \"t\" (data type) parameter value: \"' + t + '\"';
    }

    var text = text;

    this.toComputationPeriodIdentifier = function () {
        switch(text) {
        case 'DV':
            return 'Daily';
        default:
            return undefined;
        }
    } // toComputationPeriodIdentifier
} // DataType

/**
   @description TimeSeriesIdentifier prototype.
*/
var TimeSeriesIdentifier = function (text) {
    // private; no reason to modify this once the object is created
    var text = text;

    /**
       @description Make site number substring of TimeSeriesIdentifier
                    visible.
    */
    this.siteNumber = function () {
        if (text.indexOf('@') === -1) {
            return undefined;
        }
        return text.split('@')[1]; // return parsed site number
    }

    /**
       @description Make parameter substring of TimeSeriesIdentifier
                    visible.
    */
    this.parameter = function () {
        // try to parse "Parameter" field value
        var field = text.split('.');

        if (field.length < 2) {
            return;             // failure
        }
        return field[0];
    }

    /**
       @description Make LocationIdentifier substring of
                    TimeSeriesIdentifier visible.
    */
    this.locationIdentifier = function () {
        // try to parse "locationIdentifier" field value
        var field = text.split('@');

        if (field.length < 2) {
            return;             // failure
        }
        return field[1];
    }
} // TimeSeriesIdentifier

/**
   @description RDBTable prototype.
*/
var RDBTable = function (parameters, timeSeriesCorrectedData) {
    // TODO: figure out how much of this stuff gets retained (actually a
    // JIRA ticket now). nwts2rdb appears to include/omit fields in the
    // header depending on what it finds in the database.

    // convoluted syntax for "now"
    var retrieved = rfc3339((new Date()).toISOString());
    // make TimeSeriesIdentifier object from HTTP query parameter text
    var timeSeriesIdentifier =
        new TimeSeriesIdentifier(parameters.timeSeriesIdentifier);
    var agencyCode, siteNumber;

    // if locationIdentifier was not provided
    if (parameters.locationIdentifier === undefined) {
        agencyCode = 'USGS';    // default agency code
        // reference site number embedded in timeSeriesIdentifier
        siteNumber = timeSeriesIdentifier.siteNumber();
    }
    else {
        // parse (agency code, site number) embedded in
        // locationIdentifier
        var f = parameters.locationIdentifier.split('-')[0];

        agencyCode = f[1];
        siteNumber = f[0];
    }

    var header =
        '# //UNITED STATES GEOLOGICAL SURVEY       ' +
        'http://water.usgs.gov/\n' +
        '# //NATIONAL WATER INFORMATION SYSTEM     ' +
        'http://water.usgs.gov/data.html\n' +
        '# //DATA ARE PROVISIONAL AND SUBJECT TO CHANGE ' +
        'UNTIL PUBLISHED BY USGS\n' +
        '# //RETRIEVED: ' + retrieved + '\n' +
        '# //STATION AGENCY=\"' + agencyCode + ' \" NUMBER=\"' +
        siteNumber + '       \"\n' +
        '# //RANGE START=\"' + rfc3339(parameters.queryFrom) +
        '\" END=\"' + rfc3339(parameters.queryTo) + '\"\n';

    // TODO: the code that produces the RDB, column data type
    // declarations below is probably going to need to be much more
    // robust.

    // Note: a "header" is a very different thing from a "heading".
    var heading =
    'DATE\tTIME\tVALUE\tPRECISION\tREMARK\tFLAGS\tTYPE\tQA\n' +
        '8D\t6S\t16N\t1S\t1S\t32S\t1S\t1S\n';
    var n = timeSeriesCorrectedData.Points.length;
    var body = '';
    // TODO: for tables with a large number of rows, we'll probably
    // need to convert this loop to an event-driven mechanism (driven
    // by AQUARIUS response?), instead of accumulating all rows in
    // DVTable.body, and using tons of memory.
    for (var i = 0; i < n; i++) {
        // shorten some object references below
        var point = timeSeriesCorrectedData.Points[i];

        // TODO: For DVs at least (and perhaps other types), legacy
        // dates might need to be re-offset on output:

        // On Mon, Sep 28, 2015 at 3:05 PM, Scott Bartholoma
        // <sbarthol@usgs.gov> said:
        //
        // The migration exported all the data in the standard time
        // UTC offset for the site in the SITEFILE and as far as I
        // know AQUARIUS imported it that way. There was no timestamp
        // on the Daily Values exported from Adaps, so AQUARIUS had to
        // "make one up".  I'm pretty sure they used end-of-day
        // midnight, which means that for Migrated data the dates in
        // the timeseries have to be decremented.
        // "2015-01-01T00:00:00.0000000-07:00" is the value for
        // 09/30/2015.
        //
        // However, to do this properly so it will work correctly for
        // ALL data, including future setups that don't match how we
        // migrated, you have to pay attention to the interpolation
        // type from the timeseries description. We are using
        // "Preceeding Constant" where the value represents the
        // statistic for the preceeding period.  However, there is
        // also "Succeeding Constant". And to further complicate this,
        // you can select to have the value be at the beginning of the
        // day or at the end of the day (see image below).
        //
        // As I write this, I created a timeseries and then got it's
        // description. I don't see anything in the timeseries
        // description to tell us what the interpolation type is not
        // which setting was chosen in the image below was
        // chosen. Here is the json output from the
        // getTimeseriesDescriptionList call. I must be missing
        // something.

        // On Tue, Sep 29, 2015 at 10:57 AM, Scott Bartholoma
        // <sbarthol@usgs.gov> said:
        //
        // I see Interpolation Type is part of the timeseries data
        // response, not part of the timeseries description. As i
        // recall, it can be changed over time, but I wouldn't use
        // that "feature". If I wanted to change interpolation type I
        // would start a new timeseries.In any case, you might be able
        // to use it to decide if you need to decrement the date or
        // not when doing DV data.

        var d = new Date(point.Timestamp);
        // TODO: Date parse error handling goes here?

        // the daily value
        var value = point.Value.Numeric.toString();

        // TODO: ugly, ISO 8601 to RFC3339 subtype, date-reformatting
        // to re-factor eventually
        body += point.Timestamp.split('T')[0].replace(/-/g, '') +
            // TIME column always empty for daily values
            '\t\t' + value + '\t' +

        // On Tue, Sep 29, 2015 at 10:57 AM, Scott Bartholoma
        // <sbarthol@usgs.gov> said:
        //
        // Precision isn't stored in the database so it would have to
        // be derived from the numeric string returned in the json
        // output. I don't know how useful it is anymore. It was
        // mainly there for the "suppress rounding" option so the user
        // would know how many digits to round it to for rounded
        // display.
        value.toString().replace('.', '').length + '\t' +

        // On Tue, Sep 29, 2015 at 10:57 AM, Scott Bartholoma
        // <sbarthol@usgs.gov> said:
        //
        // Remark will have to be derived from the Qualifier section
        // of the response. It will have begin and end dates for
        // various qualification periods.

        // TODO: "Notes" looks like it's an array in the JSON
        // messageBody, so we might need further processing here
        timeSeriesCorrectedData.Notes + '\t' +

        // On Tue, Sep 29, 2015 at 10:57 AM, Scott Bartholoma
        // <sbarthol@usgs.gov> said:
        //
        // I think some of what used to be flags are now
        // Qualifiers. Things like thereshold exceedances (high, very
        // high, low, very low, rapid increace/decreast [sic],
        // etc.). The users might want you to put something in that
        // column for the Method and Grade sections of the response as
        // well
        '\t' +

        // TODO: need to ask Brad and/or users about preserving the
        // TYPE column (see excerpt from Scott's mail below).

        // On Tue, Sep 29, 2015 at 10:57 AM, Scott Bartholoma
        // <sbarthol@usgs.gov> said:
        //
        // Type I would put in something like "R" for raw and "C" for
        // corrected depending on which get method was used. That is
        // similar to what C (computed) and E (Edited) meant for DV
        // data in Adaps.  We don't explicitly have the Meas, Edit,
        // and Comp UV types anymore, they are separate timeseries in
        // AQUARIUS.
        '\t' +
            // TODO: FLAGS?
            '\t' +
            timeSeriesCorrectedData.Approvals[0].LevelDescription.charAt(0)
            + '\n';
    }

    this.toString = function () {
        return header + heading + body;
    } // toString
} // RDBTable

/**
   @description DVTable prototype.
*/
var DVTable = function (
    parameters, timeSeriesDescriptions
) {
    var parameters = parameters;
    var timeSeriesDescriptions = timeSeriesDescriptions;

    // TODO: would be nice to re-factor this
    /**
       @description Produce an RDB file response of daily values
       related to this TimeSeriesDescription set.
    */
    this.toRDB = function (aq2rdbResponse) {
        /**
           @description Handle response from GetTimeSeriesCorrectedData.
        */
        function callback(aquariusResponse) {
            var messageBody = '';

            // accumulate response
            aquariusResponse.on(
                'data',
                function (chunk) {
                    messageBody += chunk;
                });

            aquariusResponse.on('end', function () {
                try {
                    var timeSeriesCorrectedData = JSON.parse(messageBody);
                }
                catch (e) {
                    jsonParseErrorMessage(aq2rdbResponse, e.message);
                    return;         // go no further
                }

                if (200 < aquariusResponse.statusCode) {
                    rdbMessage(
                        aq2rdbResponse,
                        aquariusResponse.statusCode,
                        '# ' + PACKAGE_NAME +
                            ': AQUARIUS replied with an error. ' +
                            'The message was:\n' +
                            '#\n' +
                            '#   ' +
                            timeSeriesCorrectedData.ResponseStatus.Message
                    );
                }
                else {
                    // make an RDB file
                    var rdbTable =
                        new RDBTable(
                            parameters,
                            timeSeriesCorrectedData
                        );
                    aq2rdbResponse.end(rdbTable.toString());
                }
            });
        } // callback

        var n = timeSeriesDescriptions.length;
        for (var i = 0; i < n; i++) {
            var path = AQUARIUS_PREFIX + 'GetTimeSeriesCorrectedData?' +
                    bind('token', parameters.token) + '&format=json' +
                    bind('timeSeriesUniqueId',
                         timeSeriesDescriptions[i].UniqueId) +
                    bind('queryFrom', parameters.queryFrom) +
                    bind('queryTo', parameters.queryTo);
            // call GetTimeSeriesCorrectedData service to get daily
            // values associated with time series descriptions
            var request = http.request({
                host: AQUARIUS_HOSTNAME,
                path: path
            }, callback);

            /**
               @description Handle GetTimeSeriesCorrectedData service
                            invocation errors.
            */
            request.on('error', function (error) {
                log('getTimeSeriesCorrectedData.request.on(\'error\')');
            });

            request.end();
        }
    } // toRDB
} // DVTable

// New-and-improved, "Functional" inheritance pattern constructors
// start here; see *JavaScript: The Good Parts*, Douglas Crockford,
// O'Reilly Media, Inc., 2008, Sec. 5.4.

// Use GetTimeSeriesDescriptionList with the LocationIdentifier and
// Parameter parameters in the URL and then find the requested
// timeseries in the output to get tue [sic] GUID
var timeSeriesDescriptionListClass = function (spec, my) {
    var timeSeriesIdentifier =
        new TimeSeriesIdentifier(spec.timeSeriesIdentifier);

    var parameter = timeSeriesIdentifier.parameter();
    if (parameter === undefined) {
        rdbMessage(
            aq2rdbResponse, 400, 
            'Could not parse \"Parameter\" field value from ' +
                '\"timeSeriesIdentifier\" field value'
        );
        return;
    }

    var locationIdentifier = timeSeriesIdentifier.locationIdentifier();
    if (locationIdentifier === undefined) {
        rdbMessage(
            aq2rdbResponse, 400, 
            'Could not parse \"locationIdentifier\" field value from ' +
                '\"timeSeriesIdentifier\" field value'
        );
        return;
    }

    var path = AQUARIUS_PREFIX + 'GetTimeSeriesDescriptionList?' +
        bind('token', spec.token) + '&format=json' +
        bind('LocationIdentifier', locationIdentifier) +
        bind('Parameter', parameter) +
        bind('ComputationPeriodIdentifier',
             spec.computationPeriodIdentifier) + 
        '&ExtendedFilters=' +
        '[{FilterName:ACTIVE_FLAG,FilterValue:Y}]';
    var request =
        http.request({
            host: AQUARIUS_HOSTNAME,
            path: path                
        }, spec.callback);

    /**
       @description Handle GetTimeSeriesDescriptionList service
                    invocation errors.
    */
    request.on('error', function (error) {
        log('error: ' + error);
        handleService('GetTimeSeriesDescriptionList',
                      aq2rdbResponse, error);
    });

    request.end();
} // timeSeriesDescriptionListClass

var aqTokenClass = function (spec, my) {
    var that = {};

    if (spec.userName === undefined)
        throw 'Required field \"userName\" is missing';

    if (spec.password === undefined)
        throw 'Required field \"password\" is missing';

    my = my || {};

    /**
       @description GetAQToken service response callback.
    */
    function callback(response) {
        var messageBody = '';

        // accumulate response
        response.on('data', function (chunk) {
            messageBody += chunk;
        });

        // Response complete; token received.
        response.on('end', function () {
            // visibility of token string is "protected"
            my.text = messageBody;
            // if a callback function is defined
            if (spec.callback !== undefined)
                spec.callback(); // call it
        });
    } // callback

    /**
       @description GetAQToken service request for AQUARIUS
                    authentication token needed for AQUARIUS API.
    */
    var path = '/services/GetAQToken?' +
                bind('userName', spec.userName) +
                bind('password', spec.password) +
                bind('uriString',
                     'http://' + AQUARIUS_HOSTNAME + '/AQUARIUS/');
    var getAQTokenRequest =
        http.request({
            host: 'localhost',
            port: '8080',
            path: path
        }, callback);

    /**
       @description Handle GetAQToken service invocation errors.
    */
    getAQTokenRequest.on('error', function (error) {
        var statusMessage;

        if (error.message === 'connect ECONNREFUSED') {
            throw 'Could not connect to GetAQToken service for ' +
                'AQUARIUS authentication token';
        }
        else {
            throw error;
        }
    });

    getAQTokenRequest.end();

    // add shared variables and functions to my

    // add privileged methods to that
    /*
    that.getResource = function () {
        return resource;
    };
    */

    /**
       @description AQUARIUS token, as string.
    */
    that.toString = function () {
        return my.text;
    }

    return that;
} // aqTokenClass

var dvTableClass = function (spec, my) {
    var that = {};
    // private instance variables
    // AQUARIUS clerical stuff
    var aqToken;
    var environment;
    // science stuff
    var timeSeriesIdentifier;
    var queryFrom, queryTo;
    // TODO: GetTimeSeriesCorrectedData claims to accept this, but
    // not sure if these are necessary for aq2rdb yet:
    // var getParts;
    var unit, utcOffset;
    var applyRounding, computed; // TODO: these might have defaults?

    // might be useful to move out of this scope eventually
    function toBoolean(literal) {
        if (literal === 'true') {
            computed = true;
        }
        else if (literal === 'false') {
            computed = false;
        }
        else {
            throw 'Could not parse value \"'  + literal +
                '\" in \"computed\" field';
        }
    }

    // get HTTP query arguments
    for (var field in spec) {
        switch (field) {
        case 'userName':
        case 'password':
            // see aqTokenClass constructor call below
            break;
        case 'environment':
            environment = spec[field];
            break;
        case 'timeSeriesIdentifier':
            timeSeriesIdentifier = new TimeSeriesIdentifier(spec[field]);
            break;
        case 'queryFrom':
            queryFrom = spec[field]; // TODO: needs domain validation
            break;
        case 'queryTo':
            queryTo = spec[field]; // TODO: needs domain validation
            break;
        case 'unit':
            unit = spec[field];
            break;
        case 'utcOffset':
            utcOffset = spec[field];
            break;
        case 'applyRounding':
            try {
                applyRounding = toBoolean(spec[field]);
            }
            catch (error) {
                throw error;
            }
            break;
        case 'computed':
            try {
                computed = toBoolean(spec[field]);
            }
            catch (error) {
                throw error;
            }
            break;
        case 'timeOffset':
            // TODO: this should be validated as an element of a time
            // offset enumeration?
            timeOffset = spec[field];
            break;
        default:
            throw 'Unknown field \"' + field + '\"';
            return;
        }
    }

    // required fields

    if (spec.userName === undefined) {
        throw 'Required field \"userName\" is missing';
    }

    if (spec.password === undefined) {
        throw 'Required field \"password\" is missing';
    }

    // try to get AQUARIUS token from aquarius-token service
    try {
        aqToken = aqTokenClass({
            userName: spec.userName,
            password: spec.password,
            callback: callback
        });
    }
    catch (error) {
        throw error;
    }

    if (timeSeriesIdentifier === undefined)
        throw 'Required field \"timeSeriesIdentifier\" is missing';

    // TODO:
    function callback() {
    }

    // "The 'my' object is a container of secrets that are shared by
    // the constructors in the inheritance chain. The use of the 'my'
    // object is optional. If a 'my' object is not passed in, then a
    // 'my' object is made."
    my = my || {};

    // add shared variables and functions to my

    // add privileged methods to that
    /*
    that.getResource = function () {
        return resource;
    };
    */

    return that;
} // dvTable

/**
   @description GetDVTable service request handler.
*/
httpdispatcher.onGet(
    '/' + PACKAGE_NAME + '/GetDVTable',
    function (request, response) {
        // object spec. is derived from HTTP query field values
        var spec = querystring.parse(request.url);
        // this property would be just clutter in the object right now,
        // so delete it
        delete spec['/' + PACKAGE_NAME + '/GetDVTable?'];
        try {
            var dvTable = dvTableClass(spec);
        }
        catch (error) {
            rdbMessage(response, 400, error);
            return;
        }
        response.end();
});

/**
   @description GetUVTable service request handler.
*/
httpdispatcher.onGet(
    '/' + PACKAGE_NAME + '/GetUVTable',
    function (request, response) {
        // TODO:
        log('GetUVTable service called');
        response.end();
});

/**
   @description Legacy, pseudo-nwts2rdb service request handler.
*/
httpdispatcher.onGet('/' + PACKAGE_NAME, function (
    request, response
) {
    var query;

    try {
        query = new Query(request, response);
    }
    catch (error) {
        rdbMessage(response, 400, error);
        return;
    }
}); // httpdispatcher.onGet()

/**
   @description Service dispatcher.
*/ 
function handleRequest(request, response) {
    try {
        httpdispatcher.dispatch(request, response);
    }
    catch (error) {
       log(error.message);
    }
}

/**
   @description Create HTTP server to host the service.
*/ 
var server = http.createServer(handleRequest);

/**
   @description Start listening for requests.
*/ 
server.listen(PORT, function () {
    log('Server listening on: http://localhost:' + PORT.toString());
});
