/**
 * @fileOverview A Web service to map AQUARIUS, time series data
 *               requests to USGS-variant RDB files.
 *
 * @author <a href="mailto:ashalper@usgs.gov">Andrew Halper</a>
 *
 * @see <a href="https://sites.google.com/a/usgs.gov/nwis_integrator/data_retrieval/cli/aqts2rdb">aqts2rdb</a>.
 */

'use strict';
var sprintf = require("sprintf-js").sprintf;
var http = require('http');
var httpdispatcher = require('httpdispatcher');
var querystring = require('querystring');

/**
   @description The aq2rdb Web service name.
*/
var SERVICE_NAME = 'aq2rdb';

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
   @description TimeSeriesIdentifier prototype.
*/
var TimeSeriesIdentifier = function(text) {
    this.text = text;

    this.siteNumber = function () {
        // TODO: this needs to be more robust
        return this.text.split('@')[1]; // return parsed site number
    }

    this.parameter = function () {
        // try to parse "Parameter" field value
        var field = this.text.split('.');

        if (field.length < 2) {
            return;             // failure
        }
        return field[0];
    }

    this.locationIdentifier = function () {
        // try to parse "locationIdentifier" field value
        var field = this.text.split('@');

        if (field.length < 2) {
            return;             // failure
        }
        return field[1];
    }
} // TimeSeriesIdentifier

/**
   @description Set of TimeSeriesDescriptions prototype.
*/
var TimeSeriesDescriptionSet = function (
    field, timeSeriesDescriptions
) {
    // TODO: passing in the entire "field" object is probably
    // overkill; need to declare private properties for only the stuff
    // TimeSeriesDescriptionSet objects require to do their job
    var field = field;
    var timeSeriesDescriptions = timeSeriesDescriptions;

    /**
       @description Produce an RDB file response of daily values
       related to this TimeSeriesDescription set.
    */
    this.dvRespond = function (aq2rdbResponse) {
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
                    // TODO: probably want to re-factor this into a
                    // function eventually
                    var statusMessage =
                        '# ' + SERVICE_NAME +
                        ': AQUARIUS replied with an error. ' +
                        'The message was:\n' +
                        '#\n' +
                        '#   ' +
                        timeSeriesCorrectedData.ResponseStatus.Message;
                    aq2rdbResponse.writeHead(
                        aquariusResponse.statusCode, statusMessage,
                        {'Content-Length': statusMessage.length,
                         'Content-Type': 'text/plain'}
                    );
                    aq2rdbResponse.end(statusMessage);
                }
                else {
                    // make an RDB file
                    var points = timeSeriesCorrectedData.Points;
                    var n = points.length;
                    // TODO: we'll probably need an "RDB" object prototype
                    // eventually
                    var rdb = header(field);

                    // TODO: the code that produces the RDB data type
                    // declaration is probably going to need to be much
                    // more robust than this.
                    rdb +=
                    'DATE\tTIME\tVALUE\tPRECISION\tREMARK\tFLAGS\tTYPE\tQA\n' +
                        '8D\t6S\t16N\t1S\t1S\t32S\t1S\t1S\n';
                    for (var i = 0; i < n; i++) {
                        var d = new Date(points[i].Timestamp);
                        // TODO: Date parse error handling

                        rdb +=
                        // TODO: date-reformatting to re-factor eventually
                        points[i].Timestamp.split('T')[0].replace(/-/g, '') +
                            // TIME column always empty for daily values
                            '\t\t' +
                            points[i].Value.Numeric.toString() + '\t' +
                            // TODO: PRECISION?
                            '\t' +
                            // TODO: "Notes" looks like it's an array in the
                            // JSON messageBody, so we might need further
                            // processing here
                            timeSeriesCorrectedData.Notes + '\t' +
                            // TODO: FLAGS?
                            '\t' +
                            // TODO: TYPE?
                            '\t' +
                            // TODO: FLAGS?
                            '\t' +
                timeSeriesCorrectedData.Approvals[0].LevelDescription.charAt(0)
                            + '\n';
                    }
                    aq2rdbResponse.end(rdb);
                }
            });
        } // callback

        var n = timeSeriesDescriptions.length;
        for (var i = 0; i < n; i++) {
            var request = http.request({
                host: AQUARIUS_HOSTNAME,
                path: AQUARIUS_PREFIX + 'GetTimeSeriesCorrectedData?' +
                    'token=' + field.token + '&format=json' +
                    bind('timeSeriesUniqueId',
                         timeSeriesDescriptions[i].UniqueId) +
                    bind('queryFrom', field.queryFrom) +
                    bind('queryTo', field.queryTo)
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
    }
} // TimeSeriesDescriptionSet

/**
   @description Primitive logging function for debugging purposes.
*/
function log(message) {
    console.log(SERVICE_NAME + ': ' + message);
}

/**
   @description Consolodated error message writer.
*/ 
function aq2rdbErrorMessage(response, statusCode, message) {
    var statusMessage = '# ' + SERVICE_NAME + ': ' + message;
    response.writeHead(statusCode, statusMessage,
                       {'Content-Length': statusMessage.length,
                        'Content-Type': 'text/plain'});
    response.end(statusMessage);
}

function jsonParseErrorMessage(response, message) {
    aq2rdbErrorMessage(
        response, 502, 
        'While trying to parse a JSON response from ' +
            'AQUARIUS: ' + message
    );
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
   @description Retreive time series data from AQUARIUS API.
*/
function getTimeSeriesDescriptionList(field, aq2rdbResponse) {
    var timeSeriesDescriptionList;

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
            try {
                timeSeriesDescriptionList = JSON.parse(messageBody);
            }
            catch (e) {
                jsonParseErrorMessage(aq2rdbResponse, e.message);
                return;         // go no further
            }

            var timeSeriesDescriptions =
                timeSeriesDescriptionList.TimeSeriesDescriptions;
            var timeSeriesDescriptionSet =
                new TimeSeriesDescriptionSet(
                    field,
                    timeSeriesDescriptionList.TimeSeriesDescriptions
                );
            // get the DVs from AQUARIUS and respond with the RDB file
            timeSeriesDescriptionSet.dvRespond(aq2rdbResponse);
        });
    } // callback

    var timeSeriesIdentifier =
        new TimeSeriesIdentifier(field.timeSeriesIdentifier);

    var parameter = timeSeriesIdentifier.parameter();
    if (parameter === undefined) {
        aq2rdbErrorMessage(
            aq2rdbResponse, 400, 
            'Could not parse \"Parameter\" field value from ' +
                '\"timeSeriesIdentifier\" field value'
        );
    }

    var locationIdentifier = timeSeriesIdentifier.locationIdentifier();
    if (locationIdentifier === undefined) {
        aq2rdbErrorMessage(
            aq2rdbResponse, 400, 
            'Could not parse \"locationIdentifier\" field value from ' +
                '\"timeSeriesIdentifier\" field value'
        );
        return;
    }

    var path = AQUARIUS_PREFIX + 'GetTimeSeriesDescriptionList?' +
        '&token=' + field.token + '&format=json' +
        bind('Parameter', parameter) +
        '&ExtendedFilters=' +
        '[{FilterName:ACTIVE_FLAG,FilterValue:Y}]' +
        bind('LocationIdentifier', locationIdentifier);

    var request =
        http.request({
            host: AQUARIUS_HOSTNAME,
            path: path                
        }, callback);

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
} // getTimeSeriesDescriptionList

/**
   @description Convert an ISO 8601 date string to (a specific
                instance of) an RFC3339 date (for our purposes, the
                instance of which doesn't happen to have a specific
                name).
*/
function rfc3339(isoString) {
    return isoString.replace('T', ' ').replace(/\.\d*/, '');
}

// TODO: figure out how much of this stuff gets retained (actually a
// JIRA ticket now). nwts2rdb appears to include/omit fields in the
// header depending on what it finds in the database.
function header(field) {
    // convoluted syntax for "now"
    var retrieved = rfc3339((new Date()).toISOString());
    // make TimeSeriesIdentifier object from HTTP query parameter text
    var timeSeriesIdentifier =
        new TimeSeriesIdentifier(field.timeSeriesIdentifier);
    var agencyCode, siteNumber;

    // if locationIdentifier was not provided
    if (field.locationIdentifier === undefined) {
        agencyCode = 'USGS';    // default agency code
        // reference site number embedded in timeSeriesIdentifier
        siteNumber = timeSeriesIdentifier.siteNumber();
    }
    else {
        // parse (agency code, site number) embedded in
        // locationIdentifier
        var f = field.locationIdentifier.split('-')[0];

        agencyCode = f[1];
        siteNumber = f[0];
    }

    var header =
    '# //UNITED STATES GEOLOGICAL SURVEY       ' +
        'http://water.usgs.gov/\n' +
    '# //NATIONAL WATER INFORMATION SYSTEM     ' +
        'http://water.usgs.gov/data.html\n' +
    '# //DATA ARE PROVISIONAL AND SUBJECT TO CHANGE UNTIL PUBLISHED BY USGS\n' +
    '# //RETRIEVED: ' + retrieved + '\n' +
    '# //STATION AGENCY=\"' + agencyCode + ' \" NUMBER=\"' +
        siteNumber + '\n' +
    '# //RANGE START=\"' + rfc3339(field.queryFrom) +
        '\" END=\"' + rfc3339(field.queryTo) + '\"\n';

    return header;
}

/**
   @description Figure out what the aq2rdb request is, then call the
                necessary AQUARIUS API services to accomplish it.
*/
function aquariusDispatch(token, arg, aq2rdbResponse) {
    var field = {
        // some defaults
        token: token,
        environment: 'production',
        applyRounding: true,
        computed: false,
        timeOffset: 'LOC',
        combineDateAndTime: true
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
            field.environment = arg[opt];
            break;
        case 'a':
            field.agencyCode = arg[opt];
            break;
        case 'b':
            field.queryFrom = arg[opt];
            break;
        case 'n':
            // AQUARIUS seems to do ill-advised, implicit things with
            // site PK
            field.locationIdentifier =
                arg.a === undefined ? arg[opt] : arg[opt] + '-' + arg.a;
            break;
        case 'u':
            // -u is a new option for specifying a time-series
            // identifier, and is preferred over -d whenever possible.
            // If used, -a, -n, -t, -s, -d, and -p are ignored.
            field.timeSeriesIdentifier = arg[opt];
            break;
        case 'd':
            // -d data descriptor number is supported but is
            // deprecated. This option will work for time series that
            // were migrated from ADAPS. This will *not work for new
            // time series* created in AQUARIUS (relies on ADAPS_DD
            // time-series extended attribute). We prefer you use -u
            // whenever possible.
            ddID = arg[opt];
            break;
        case 'r':
            field.applyRounding = false;
            break;
        case 'c':
            // For [data] type "dv", Output COMPUTED daily values
            // only. For other types except pseudo-UV retrievals,
            // combine date and time in a single column.
            field.computed = true;
            break;
        case 't':
            dataType = arg[opt].toUpperCase();
            break;
        case 'l':
            field.timeOffset = arg[opt];
            break;
        case 'e':
            field.queryTo = arg[opt];
            break;
        }
    }

    // begin date validation
    if (field.queryFrom !== undefined) {
        // if (AQUARIUS) queryFrom (aq2rdb "b") field is not a valid
        // ISO date string
        if (isNaN(Date.parse(field.queryFrom))) {
            aq2rdbErrorMessage(
                aq2rdbResponse, 400,
                SERVICE_NAME +
                    ': If \"b\" is specified, a valid 14-digit ISO ' +
                    'date must be provided.'
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
        (field.locationIdentifier === undefined ||
         ddID === undefined ||
         field.transportCode === undefined)) {
        aq2rdbErrorMessage(
            aq2rdbResponse, 400,
            '\"n\" and \"d\" fields ' +
                'must be present when \"t\" is \"MEAS\"'
        );
        return;
    }

    // set expand rating flag as a character
    // TODO: "-e" was maybe an "overloaded" option in nwts2rdb? Need
    // to find out if this is still relevant.
    /*
      if (eflag) {
      strcpy (expandit,'Y');
      }
      else {
      strcpy (expandit,'N');
      }
    */

    // if time-series identifier is not present, and location
    // identifier is present
    if (field.timeSeriesIdentifier !== undefined) {
        // Use GetTimeSeriesDescriptionList with the
        // LocationIdentifier and Parameter parameters in the URL and
        // then find the requested timeseries in the output to get tue
        // [sic] GUID
        getTimeSeriesDescriptionList(field, aq2rdbResponse);

        // Use GetTimeSeriesRawDaa [sic] or getTimeSeriesCorrectedData
        // with the TimeSeriesUniqueId parameter to get the data.
        // getTimeSeriesCorrectedData(field, aq2rdbResponse);
    }
} // aquariusDispatch

/**
   @description Service GET request handler.
*/ 
httpdispatcher.onGet('/' + SERVICE_NAME, function (
    aq2rdbRequest, aq2rdbResponse
) {
    // parse HTTP query parameters in GET request URL
    var arg = querystring.parse(aq2rdbRequest.url);
    var statusMessage;

    // some pre-validation to see if it's worthwhile to call
    // GetAQToken to proceed to the next step

    if (arg.userName.length === 0) {
        aq2rdbErrorMessage(
            aq2rdbResponse, 400,
            '# ' + SERVICE_NAME + ': Required parameter ' +
                '\"userName\" not found'
        );
        return;
    }
    var userName = arg.userName;

    if (arg.password.length === 0) {
        aq2rdbErrorMessage(
            aq2rdbResponse, 400,
            '# ' + SERVICE_NAME + ': Required parameter ' +
                '\"password\" not found'
        );
        return;
    }
    var password = arg.password;

    // data type ("t") parameter domain validation
    switch (arg.t.toUpperCase()) {
    case 'MS':
        statusMessage =
            'Pseudo-time series (e.g., gage inspections) are not supported';
        break;
    case 'VT':
        statusMessage = 'Sensor inspections and readings are not supported';
        break;
    case 'PK':
        statusMessage = 'Peak-flow data are not supported';
        break;
    case 'DC':
        statusMessage = 'Data corrections are not supported';
        break;
    case 'SV':
        statusMessage = 'Quantitative site-visit data are not supported';
        break;
    case 'WL':
        statusMessage = 'Discrete groundwater-levels data are not supported';
        break;
    case 'QW':
        statusMessage = 'Discrete water quality data are not supported';
        break;
    // these are the only valid "t" parameter values right now
    case 'DV':
    case 'UV':
        break;
    default:
        statusMessage =
            'Unknown \"t\" (data type) parameter value: \"' + t +
            '\"';
    }

    if (statusMessage !== undefined) {
        // there was an error
        aq2rdbErrorMessage(aq2rdbResponse, 400, statusMessage);
    }

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
            aquariusDispatch(messageBody, arg, aq2rdbResponse);
        });
    } // getAQTokenCallback

    /**
       @description GetAQToken service request for AQUARIUS
                    authentication token needed for AQUARIUS API.
    */
    var request =
        http.request({
            host: 'localhost',
            port: '8080',
            path: '/services/GetAQToken?userName=' +
                userName + '&password=' + password +
                '&uriString=http://' + AQUARIUS_HOSTNAME + '/AQUARIUS/'
        }, getAQTokenCallback);

    /**
       @description Handle GetAQToken service invocation errors.
    */
    request.on('error', function (error) {
        if (error.message === 'connect ECONNREFUSED') {
            statusMessage = '# ' + SERVICE_NAME +
                ': Could not connect to GetAQToken service for ' +
                'AQUARIUS authentication token';

            aq2rdbResponse.writeHead(504, statusMessage,
                               {'Content-Length': statusMessage.length,
                                'Content-Type': 'text/plain'});
        }
        else {
            log('error.message: ' + error.message);
        }
        log('statusMessage: ' + statusMessage);
        aq2rdbResponse.end(statusMessage);
    });

    request.end();
}); // httpdispatcher.onGet()

/**
   @description Service dispatcher (there is only one path to
                dispatch).
*/ 
function handleRequest(request, response) {
    try {
        httpdispatcher.dispatch(request, response);
    }
    catch (error) {
        log('error.message: ' + error.message);
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
