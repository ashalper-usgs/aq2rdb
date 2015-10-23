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
var async = require('async');

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
function rdbMessage(response, error) {
    var statusMessage = '# ' + PACKAGE_NAME + ': ' + error.message;

    response.writeHead(error.statusCode, statusMessage,
                       {'Content-Length': statusMessage.length,
                        'Content-Type': 'text/plain'});
    response.end(statusMessage);
}

/**
   @description Convert an ISO 8601 extended format, date string to
                basic format.
*/
function toBasicFormat(s) {
    return s.replace('T', ' ').replace(/\.\d*/, '');
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
        response,
        {statusCode: 502, 
         message: 'While trying to parse a JSON response from ' +
                  'AQUARIUS: ' + message}
    );
}

/**
   @description DataType prototype.
*/ 
var DataType = function (text) {
    // data type ("t") parameter domain validation
    switch (text) {
    case 'MS':
        throw {statusCode: 400,
               message:
               'Pseudo-time series (e.g., gage inspections) are not supported'};
        break;
    case 'VT':
        throw {statusCode: 400,
               message: 'Sensor inspections and readings are not supported'};
        break;
    case 'PK':
        throw {statusCode: 400,
               message: 'Peak-flow data are not supported'};
        break;
    case 'DC':
        throw {statusCode: 400,
               message: 'Data corrections are not supported'};
        break;
    case 'SV':
        throw {statusCode: 400,
               message: 'Quantitative site-visit data are not supported'};
        break;
    case 'WL':
        throw {statusCode: 400,
               message: 'Discrete groundwater-levels data are not supported'};
        break;
    case 'QW':
        throw {statusCode: 400,
               message: 'Discrete water quality data are not supported'};
        break;
    // these are the only valid "t" parameter values right now
    case 'DV':
    case 'UV':
        break;
    default:
        throw {statusCode: 400,
               message: 'Unknown \"t\" (data type) parameter ' +
                        'value: \"' + t + '\"'};
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
   @description TimeSeriesIdentifier object prototype.
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
   @description ISO 8601 "basic format" date prototype.
   @see https://en.wikipedia.org/wiki/ISO_8601#General_principles
*/
var BasicFormat = function (text) {
    var text = text;

    /**
       @description Convert basic format date to extended format date.
    */
    function basicToExtended(text) {
        return text.substr(0, 4) + '-' + text.substr(4, 2) + '-' +
            text.substr(6, 2);
    }

    // re-format as ISO "extended format" for Date.parse() purposes
    var datestring = basicToExtended(text);

    if (isNaN(Date.parse(datestring))) {
        throw {statusCode: 400,
               message: 'Could not parse date string \"' + text + '\"'};
    }

    /**
       @description Convert ISO basic format to combined extended
                    format, referenced to a specified point type.
    */
    this.toCombinedExtendedFormat = function (pointType) {
        switch (pointType) {
        case 'S':
            // second
            return basicToExtended(text) + 'T00:00:00';
        }
    } // toCombinedExtendedFormat

    this.toString = function () {
        return text;
    } // toString

} // BasicFormat

function getTimeSeriesDescriptionList(
    timeSeriesDescriptionServiceRequest, callback
) {
    /**
       @description Callback to handle response from
                    GetTimeSeriesDescriptionList service.
    */
    function getTimeSeriesDescriptionListCallback(response) {
        var messageBody = '';

        // accumulate response
        response.on(
            'data',
            function (chunk) {
                messageBody += chunk;
            });

        response.on('end', function () {
            var timeSeriesDescriptionListServiceResponse;

            try {
                timeSeriesDescriptionServiceResponse =
                    JSON.parse(messageBody);
            }
            catch (error) {
                throw {statusCode: 502, message: error};
            }

            // TODO:
            // {"ResponseStatus":{"ErrorCode":"ModelNotFoundException","Message":"Discharge.ft^3/s.Mean is not a valid parameter","Errors":[]}}
            log(timeSeriesDescriptionServiceResponse.ResponseStatus.ErrorCode);

            // if the GetTimeSeriesDescriptionList query returned no
            // time series descriptions
            if (timeSeriesDescriptionServiceResponse.TimeSeriesDescriptions ===
                undefined) {
                // there's nothing more we can do
                rdbMessage(
                    response,
                    {statusCode: 200,
                     message: 'The query found no time series ' +
                     'descriptions in AQUARIUS'}
                );
                return;
            }

            callback(
                timeSeriesDescriptionServiceResponse.TimeSeriesDescriptions
            );
        });
    } // getTimeSeriesDescriptionListCallback

    // the path part of GetTimeSeriesDescriptionList URL
    var path = AQUARIUS_PREFIX + 'GetTimeSeriesDescriptionList?' +
        bind('token', timeSeriesDescriptionServiceRequest.token) +
        bind('format', 'json') +
        bind('LocationIdentifier',
             timeSeriesDescriptionServiceRequest.locationIdentifier) +
        bind('Parameter',
             timeSeriesDescriptionServiceRequest.parameter) +
        bind('ExtendedFilters',
             timeSeriesDescriptionServiceRequest.extendedFilters);
    log('path: ' + path);
    var request = http.request({
        host: AQUARIUS_HOSTNAME,
        path: path                
    }, getTimeSeriesDescriptionListCallback);

    /**
       @description Handle GetTimeSeriesDescriptionList service
                    invocation errors.
    */
    request.on('error', function (error) {
        throw {statusCode: 502, message: error};
    });

    request.end();
} // getTimeSeriesDescriptionList

/**
   @description Get AQUARIUS authentication token from GetAQToken
                service.
*/
function getAQToken(userName, password, callback) {
    /**
       @description GetAQToken service response callback.
    */
    function getAQTokenCallback(response) {
        var messageBody = '';

        // accumulate response
        response.on('data', function (chunk) {
            messageBody += chunk;
        });

        // Response complete; token received.
        response.on('end', function () {
            callback(messageBody); // send token
        });
    } // getAQTokenCallback

    /**
       @description GetAQToken service request for AQUARIUS
                    authentication token needed for AQUARIUS API.
    */
    var path = '/services/GetAQToken?' +
        bind('userName', userName) +
        bind('password', password) +
        bind('uriString',
             'http://' + AQUARIUS_HOSTNAME + '/AQUARIUS/');

    var request = http.request({
        host: 'localhost',
        port: '8080',
        path: path
    }, getAQTokenCallback);

    /**
       @description Handle GetAQToken service invocation errors.
    */
    request.on('error', function (error) {
        var statusMessage;

        if (error.message === 'connect ECONNREFUSED') {
            throw {statusCode: 502,
                   message: 'Could not connect to GetAQToken service for ' +
                   'AQUARIUS authentication token'};
        }
        else {
            throw {statusCode: 502, message: error};
        }
    });

    request.end();
} // getAQToken

/**
   @description GetDVTable service request handler.
*/
httpdispatcher.onGet('/' + PACKAGE_NAME + '/GetDVTable', function (
    request, response
) {
    log(request.url);
    var field;
    // parse HTTP query
    try {
        field = querystring.parse(request.url);
    }
    catch (error) {
        throw {statusCode: 400, message: error};
    }
    
    // this field is not used
    delete field['/' + PACKAGE_NAME + '/GetDVTable?'];
    var getDVTable = new Array();

    for (var name in field) {
        // GetAQToken service fields
        if (name.match(/userName|password|environment/)) {
            getDVTable[name] = field[name];
        }
        // AQUARIUS service fields
        else if (name.match(/LocationIdentifier|Parameter|Publish|ComputationIdentifier|ComputationPeriodIdentifier|QueryFrom|QueryTo/)) {
            // Convert capitalized AQUARIUS field names to
            // uncapitalized, JavaScript property names; annoying, but
            // we're trying to be consistent with the JavaScript
            // style.
            getDVTable[name.charAt(0).toLowerCase() + name.substr(1)] =
                field[name];
        }
        else {
            throw {statusCode: 400, message: 'Unknown field "' + name + '"'};
        }
    }

    if (getDVTable.userName.length === 0) {
        throw {statusCode: 400,
               message: 'Required parameter \"userName\" not found'};
    }

    if (getDVTable.password.length === 0) {
        throw {statusCode: 400,
               message: 'Required parameter \"password\" not found'};
    }

    function receiveTimeSeriesDescriptions(timeSeriesDescriptions) {
        log(JSON.stringify(timeSeriesDescriptions));
    } // receiveTimeSeriesDescriptions

    function receiveAQToken(token) {
        log('token: ' + token);
        getDVTable.token = token;

        getTimeSeriesDescriptionList(
            {token: getDVTable.token,
             locationIdentifier: getDVTable.locationIdentifier,
             parameter: getDVTable.parameter,
             extendedFilters:
                 '[{FilterName:ACTIVE_FLAG,FilterValue:Y}]'},
            receiveTimeSeriesDescriptions
        );
    } // receiveAQToken

    try {
        getAQToken(
            getDVTable.userName, getDVTable.password, receiveAQToken
        );
    }
    catch (error) {
        throw error;
    }

    response.end();
});

/**
   @description GetUVTable service request handler.
*/
httpdispatcher.onGet('/' + PACKAGE_NAME + '/GetUVTable', function (
    request, response
) {
    // TODO:
    log('GetUVTable service called');
    response.end();
});

/**
   @description Legacy, pseudo-nwts2rdb service request handler. Use
                HTTP query fields to decipher the desired aq2rdb
                request, then call the necessary AQUARIUS API services
                to accomplish it.
*/
httpdispatcher.onGet('/' + PACKAGE_NAME, function (
    request, response
) {
    // parse HTTP query
    var arg = querystring.parse(request.url);
    var userName, password;

    if (arg.userName.length === 0) {
        throw {statusCode: 400,
               message: 'Required parameter "userName" not found'};
    }
    userName = arg.userName;

    if (arg.password.length === 0) {
        throw {statusCode: 400,
               message: 'Required parameter "password" not found'};
    }
    password = arg.password;

    // TODO: this might need to be factored-out eventually
    if (arg.u !== undefined &&
        (arg.a !== undefined || arg.n !== undefined ||
         arg.t !== undefined || arg.s !== undefined ||
         arg.d !== undefined || arg.r !== undefined ||
         arg.p !== undefined)
       ) {
        throw {statusCode: 400,
               message: 'If "u" is specified, "a", "n", "t", "s", ' +
               '"d", "r", and "p" must be omitted'};
    }

    var environment = 'production';
    var agencyCode, siteNumber, locationIdentifier, parameter;
    var t, d, b, e, c, r, l;

    // get HTTP query arguments
    for (var opt in arg) {
        switch (opt) {
        case 'z':
            // -z now indicates environment, not database number. In
            // AQUARIUS Era, logical database numbers have been
            // superseded by named time-series environments. The
            // default is 'production', and will work fine unless you
            // know otherwise.
            environment = arg[opt];
            break;
        case 'a':
            agencyCode = arg[opt];
            break;
        case 'b':
            // TODO: need to worry about the (nwts2rdb-defaulted) time
            // offset at some point
            try {
                b = new BasicFormat(arg[opt]);
            }
            catch (error) {
                throw {statusCode: 400,
                       message: 'If "b" is specified, a valid ISO ' +
                                'basic format date must be provided'};
            }
            break;
        case 'n':
            locationIdentifier = siteNumber = arg[opt];
            break;
        case 'd':
            // -d data descriptor number is supported but is
            // deprecated. This option will work for time series that
            // were migrated from ADAPS. This will *not work for new
            // time series* created in AQUARIUS (relies on ADAPS_DD
            // time-series extended attribute). We prefer you use -u
            // whenever possible.
            d = parseInt(arg[opt]);
            if (isNaN(d))
                throw {statusCode: 400,
                       message: 'Data descriptor ("d") field must be ' +
                                'an integer'};
            break;
        case 'p':
            parameter = arg[opt]; // parameter code
            break;
        case 'r':
            r = false;
            break;
        case 'c':
            // For [data] type "dv", Output COMPUTED daily values
            // only. For other types except pseudo-UV retrievals,
            // combine date and time in a single column.
            c = true;
            break;
        case 't':
            try {
                t = new DataType(arg[opt].toUpperCase());
            }
            catch (error) {
                rdbMessage(response, error);
                return;
            }
            break;
        case 'l':
            l = arg[opt];
            break;
        case 'e':
            // TODO: need to worry about the (nwts2rdb-defaulted) time
            // offset at some point
            try {
                e = new BasicFormat(arg[opt]);
            }
            catch (error) {
                throw {statusCode: 400,
                       message: 'If "e" is specified, a valid ISO ' +
                                'basic format date must be provided'};
            }
            break;
        }
    }

    // if locationIdentifier was not provided
    if (locationIdentifier === undefined)
        locationIdentifier = siteNumber; // it's the site number

    // AQUARIUS appears to have some wacky implicit logic for its
    // LocationIdentifier; seems to only append "-<agency_cd>" to
    // LocationIdentifier WHERE agency_cd <> 'USGS'
    if (agencyCode !== undefined && agencyCode !== 'USGS')
        locationIdentifier += '-' + agencyCode;

    /**
       @see https://github.com/caolan/async
    */
    async.waterfall([
        /**
           @description node-async waterfall function to get AQUARIUS
                        authentication token from GetAQToken service.
        */
        function (callback) {
            try {
                getAQToken(userName, password, callback);
            }
            catch (error) {
                throw error;
            }
        },
        /**
           @description node-async waterfall function to query
                        GetTimeSeriesDescriptionList service.
        */
        function (token, callback) {
            var extendedFilters =
                '[{FilterName:ADAPS_DD,FilterValue:' +
                eval(d === undefined ? '1' : d) + '}]';

            // TODO: "parameter" value here doesn't match
            // up with what AQUARIUS is expecting; will need mapping
            // from PARM.parm_cd.
            // 
            // On Wed, Oct 21, 2015 at 11:30 AM, Scott Bartholoma
            // said:
            // 
            // It is not yet available, and will not be available
            // from the AQUARIUS API.  This quarterly reference
            // list update we are adding rows to the PARM_ALIAS
            // table to handle the mapping.   For each timeseries
            // NWIS parameter there will be AQPARM, AQUNIT,
            // AQNAME, and AQDESC rows.  Once this is released the
            // plan it so stand up a USGS web service that uses
            // that table to answer the questions "Given this NWIS
            // parm what is the AQ parm and unit?" and "Given this
            // AQ parm and unit, what is the NWIS parm?" but this
            // is not yet available.  I don't know what the
            // proposed deployment date is for this.
            getTimeSeriesDescriptionList(
                token, locationIdentifier, parameter, extendedFilters,
                callback
            );
        },
        /**
           @description node-async waterfall function to query
                        GetTimeSeriesCorrectedData service.
        */
        function (token, timeSeriesDescriptions, callback) {
            /**
               @description Callback to handle response from
                            GetTimeSeriesCorrectedData service.
            */
            function getTimeSeriesCorrectedDataCallback(aquariusResponse) {
                var messageBody = '';
                var timeSeriesCorrectedData;

                // accumulate response
                aquariusResponse.on(
                    'data',
                    function (chunk) {
                        messageBody += chunk;
                    });

                aquariusResponse.on('end', function () {
                    try {
                        timeSeriesCorrectedData =
                            JSON.parse(messageBody);
                    }
                    catch (error) {
                        throw error;
                    }

                    if (200 < aquariusResponse.statusCode) {
                        rdbMessage(
                            response,
                            {statusCode: aquariusResponse.statusCode,
                             message: 'AQUARIUS replied with an error. ' +
                                      'The message was:\n' +
                                      '#\n' +
                                      '#   ' +
                             timeSeriesCorrectedData.ResponseStatus.Message}
                        );
                        return;
                    }

                    // TODO: might need to be in an async.sequence()?
                    callback(null, timeSeriesCorrectedData);
                });
            } // getTimeSeriesCorrectedDataCallback

            // waterfall within a waterfall; a nested waterfall
            async.waterfall([
                /**
                   @description node-async waterfall function to query
                                GetLocationData service.
                */
                function (callback) {
                    /**
                       @description Callback to handle response from
                                    GetLocationData service.
                    */
                    function getLocationDataCallback(response) {
                        var messageBody = '';

                        // accumulate response
                        response.on('data', function (chunk) {
                            messageBody += chunk;
                        });

                        // response complete
                        response.on('end', function () {
                            /**
                               @description Response from
                                            LocationDataService (a
                                            JSON string).
                            */
                            var locationDataServiceResponse;

                            try {
                                // parse response
                                locationDataServiceResponse =
                                    JSON.parse(messageBody);
                            }
                            catch (error) {
                                throw error;
                            }

                            // pass location name to header production
                            // waterfall below
                            callback(
                                null,
                                locationDataServiceResponse.LocationName
                            );
                        });
                    } // getLocationDataCallback

                    var path = AQUARIUS_PREFIX +
                        'GetLocationData?' +
                        bind('token', token) +
                        bind('format', 'json') +
                        bind('LocationIdentifier', locationIdentifier);
                    log('path: ' + path);
                    var request = http.request({
                        host: AQUARIUS_HOSTNAME,
                        path: path                
                    }, getLocationDataCallback);

                    /**
                       @description Handle GetLocationData service errors.
                    */
                    request.on('error', function (error) {
                        throw error;
                    });

                    request.end();
                },
                function (locationName, callback) {
                    // some convoluted syntax for "now"
                    var retrieved = toBasicFormat((new Date()).toISOString());

                    response.write(
                        '# //UNITED STATES GEOLOGICAL SURVEY       ' +
                            'http://water.usgs.gov/\n' +
                            '# //NATIONAL WATER INFORMATION SYSTEM     ' +
                            'http://water.usgs.gov/data.html\n' +
                            '# //DATA ARE PROVISIONAL AND ' +
                            'SUBJECT TO CHANGE UNTIL PUBLISHED ' +
                            'BY USGS\n' +
                            '# //RETRIEVED: ' + retrieved + '\n' +
                            '# //FILE TYPE="NWIS-I DAILY-VALUES" ' +
                            'EDITABLE=NO\n' +
                            '# //STATION AGENCY="' + agencyCode +
                            ' " NUMBER="' + siteNumber +
                            '       "\n' +
                            '# //STATION NAME="' + locationName + '"\n' +
                            '# //RANGE START="' + b.toString() +
                            '" END="' + e.toString() + '"\n'
                    );

                    // RDB table heading (which is different than a
                    // header).
                    response.write(
                        'DATE\tTIME\tVALUE\tPRECISION\tREMARK\t' +
                            'FLAGS\tTYPE\tQA\n' +
                            '8D\t6S\t16N\t1S\t1S\t32S\t1S\t1S\n'
                    );
                }
            ]);

            var n = timeSeriesDescriptions.length;
            for (var i = 0; i < n; i++) {
                var path = AQUARIUS_PREFIX +
                    'GetTimeSeriesCorrectedData?' +
                    bind('token', token) +
                    bind('format', 'json') +
                    bind('timeSeriesUniqueId',
                         timeSeriesDescriptions[i].UniqueId) +
                    bind('queryFrom', b.toCombinedExtendedFormat('S')) +
                    bind('queryTo', e.toCombinedExtendedFormat('S'));
                // call GetTimeSeriesCorrectedData service to get
                // daily values associated with time series
                // descriptions
                var request = http.request({
                    host: AQUARIUS_HOSTNAME,
                    path: path
                }, getTimeSeriesCorrectedDataCallback);

                /**
                   @description Handle GetTimeSeriesCorrectedData
                                service invocation errors.
                */
                request.on('error', function (error) {
                    throw error;
                });

                request.end();
            }
        },
        /**
           @description node-async waterfall function to write RDB
                        table rows.
        */
        function (timeSeriesCorrectedData, callback) {
            var n = timeSeriesCorrectedData.Points.length;
            // TODO: for tables with a very large number of rows, we'll
            // probably need to convert this loop to an event-driven
            // mechanism (driven by AQUARIUS response?), instead of
            // accumulating all RDB table rows, and possibly using tons of
            // memory.
            for (var i = 0; i < n; i++) {
                // shorten some object identifier references below
                var point = timeSeriesCorrectedData.Points[i];

                // TODO: For DVs at least (and perhaps other types),
                // legacy dates might need to be re-offset on output:

                // On Mon, Sep 28, 2015 at 3:05 PM, Scott Bartholoma
                // <sbarthol@usgs.gov> said:
                //
                // The migration exported all the data in the standard
                // time UTC offset for the site in the SITEFILE and as far
                // as I know AQUARIUS imported it that way. There was no
                // timestamp on the Daily Values exported from Adaps, so
                // AQUARIUS had to "make one up".  I'm pretty sure they
                // used end-of-day midnight, which means that for Migrated
                // data the dates in the timeseries have to be
                // decremented.  "2015-01-01T00:00:00.0000000-07:00" is
                // the value for 09/30/2015.
                //
                // However, to do this properly so it will work correctly
                // for ALL data, including future setups that don't match
                // how we migrated, you have to pay attention to the
                // interpolation type from the timeseries description. We
                // are using "Preceeding Constant" where the value
                // represents the statistic for the preceeding period.
                // However, there is also "Succeeding Constant". And to
                // further complicate this, you can select to have the
                // value be at the beginning of the day or at the end of
                // the day (see image below).
                //
                // As I write this, I created a timeseries and then got
                // it's description. I don't see anything in the
                // timeseries description to tell us what the
                // interpolation type is not which setting was chosen in
                // the image below was chosen. Here is the json output
                // from the getTimeseriesDescriptionList call. I must be
                // missing something.

                // On Tue, Sep 29, 2015 at 10:57 AM, Scott Bartholoma
                // <sbarthol@usgs.gov> said:
                //
                // I see Interpolation Type is part of the timeseries data
                // response, not part of the timeseries description. As i
                // recall, it can be changed over time, but I wouldn't use
                // that "feature". If I wanted to change interpolation
                // type I would start a new timeseries.In any case, you
                // might be able to use it to decide if you need to
                // decrement the date or not when doing DV data.

                var d = new Date(point.Timestamp);
                // TODO: Date parse error handling goes here?

                // the daily value
                var value = point.Value.Numeric.toString();

                // TODO: ugly, ISO 8601 to RFC3339 subtype,
                // date-reformatting to re-factor eventually
                response.write(
                    point.Timestamp.split('T')[0].replace(/-/g, '') +
                        // TIME column will always be empty for daily
                        // values
                        '\t\t' + value + '\t' +

                    // On Tue, Sep 29, 2015 at 10:57 AM, Scott Bartholoma
                    // <sbarthol@usgs.gov> said:
                    //
                    // Precision isn't stored in the database so it would
                    // have to be derived from the numeric string returned
                    // in the json output. I don't know how useful it is
                    // anymore. It was mainly there for the "suppress
                    // rounding" option so the user would know how many
                    // digits to round it to for rounded display.
                    value.toString().replace('.', '').length + '\t' +

                    // On Tue, Sep 29, 2015 at 10:57 AM, Scott Bartholoma
                    // <sbarthol@usgs.gov> said:
                    //
                    // Remark will have to be derived from the Qualifier
                    // section of the response. It will have begin and end
                    // dates for various qualification periods.

                    // TODO: "Notes" looks like it's an array in the JSON
                    // messageBody, so we might need further processing
                    // here
                    timeSeriesCorrectedData.Notes + '\t' +

                    // On Tue, Sep 29, 2015 at 10:57 AM, Scott Bartholoma
                    // <sbarthol@usgs.gov> said:
                    //
                    // I think some of what used to be flags are now
                    // Qualifiers. Things like thereshold exceedances
                    // (high, very high, low, very low, rapid
                    // increace/decreast [sic], etc.). The users might
                    // want you to put something in that column for the
                    // Method and Grade sections of the response as well
                    '\t' +

                    // TODO: need to ask Brad and/or users about
                    // preserving the TYPE column (see excerpt from
                    // Scott's mail below).

                    // On Tue, Sep 29, 2015 at 10:57 AM, Scott Bartholoma
                    // <sbarthol@usgs.gov> said:
                    //
                    // Type I would put in something like "R" for raw and
                    // "C" for corrected depending on which get method was
                    // used. That is similar to what C (computed) and E
                    // (Edited) meant for DV data in Adaps.  We don't
                    // explicitly have the Meas, Edit, and Comp UV types
                    // anymore, they are separate timeseries in AQUARIUS.
                    '\t' +
                        // TODO: FLAGS?
                        '\t' +
                timeSeriesCorrectedData.Approvals[0].LevelDescription.charAt(0)
                        + '\n'
                );
            }
            response.end();
        }
    ]);
}); // httpdispatcher.onGet()

/**
   @description Service dispatcher.
*/ 
function handleRequest(request, response) {
    try {
        httpdispatcher.dispatch(request, response);
    }
    catch (error) {
        if (error.statusCode !== undefined && error.message !== undefined) {
            rdbMessage(response, error);
        }
        else {
            throw error;
        }
    }
    response.end();
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
