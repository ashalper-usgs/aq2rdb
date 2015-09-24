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
   @description Primitive logging function for debugging purposes.
*/
function log(message) {
    console.log(SERVICE_NAME + ': ' + message);
}

/**
   @description Consolodated error message writer.
*/ 
function aq2rdbErrorMessage(response, statusCode, message) {
    var statusMessage = '# ' + message;
    response.writeHead(statusCode, statusMessage,
                       {'Content-Length': statusMessage.length,
                        'Content-Type': 'text/plain'});
    response.end(statusMessage);
}

function aquariusErrorMessage(response) {
    var statusMessage = '';

    // TODO: for some of these, AQUARIUS may return JSON with more
    // information; need to check for that.
    switch (response.statusCode) {
    case 301:
    case 400:
    case 401:
        statusMessage =
            '# There was a problem forwarding the data ' +
            'request to AQUARIUS. The message was:\n' +
            '#\n' +
            '#   ' + response.statusCode.toString() + ' ' +
            response.statusMessage;
        return statusMessage;
    }
}

/**
   @description Retreive time series data from AQUARIUS API.
*/
function getTimeSeriesDescriptionList(field, aq2rdbResponse) {
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
            var timeSeriesDescriptionList = JSON.parse(messageBody);
            var timeSeriesDescriptions =
                timeSeriesDescriptionList.TimeSeriesDescriptions;
            aq2rdbResponse.end(JSON.stringify(timeSeriesDescriptions));
        });
    } // callback

    var request =
        http.request({
            host: AQUARIUS_HOSTNAME,
            path:
                '/AQUARIUS/Publish/V2/GetTimeSeriesDescriptionList?' +
                'token=' + field.token + '&format=json' +
                '&Parameter=' + field.parameter +
                '&ExtendedFilters=' +
                '[{FilterName:ACTIVE_FLAG,FilterValue:Y}]' +
                '&locationIdentifier=' + field.locationIdentifier
        }, callback);

    /**
       @description Handle GetTimeSeriesDescriptionList service
                    invocation errors.
    */
    request.on('error', function (error) {
        handleService('GetTimeSeriesDescriptionList',
                      aq2rdbResponse, error);
    });

    request.end();
} // getTimeSeriesDescriptionList

// see
// https://sites.google.com/a/usgs.gov/aquarius-api-wiki/publish/gettimeseriescorrecteddata
function getTimeSeriesCorrectedData(field, aq2rdbResponse) {
    /**
       @description Handle response from GetTimeSeriesCorrectedData.
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
            var timeSeriesCorrectedData = JSON.parse(messageBody);
            var statusMessage;

            if (200 < response.statusCode) {
                statusMessage =
                    '# ' + SERVICE_NAME +
                    ': AQUARIUS replied with an error. ' +
                    'The message was:\n' +
                    '#\n' +
                    '#   ' +
                    timeSeriesCorrectedData.ResponseStatus.Message;
                aq2rdbResponse.writeHead(
                    response.statusCode, statusMessage,
                    {'Content-Length': statusMessage.length,
                     'Content-Type': 'text/plain'}
                );
                aq2rdbResponse.end(statusMessage);
            }
            else {
                aq2rdbResponse.end(messageBody);
            }
        });
    } // callback

    var path = '/AQUARIUS/Publish/V2/GetTimeSeriesCorrectedData?' +
            'token=' + field.token + '&format=json' +
            bind('timeSeriesIdentifier', field.timeSeriesIdentifier) +
            bind('queryFrom', field.queryFrom) +
            bind('queryTo', field.queryTo);

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
} // getTimeSeriesCorrectedData

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
        // TODO: might be deprecated; awaiting reply from Brad Garner
        // <bdgarner@usgs.gov>.
        case 'y':
            field.transportCode = arg[opt];
            break;
        case 'e':
            field.queryTo = arg[opt];
            break;
        }
    }

    // check for argument completion

    // date-time validation
    if (field.queryFrom !== undefined) {
        // if queryFrom ("b" field) is not a valid ISO date string
        if (isNaN(Date.parse(field.queryFrom))) {
            aq2rdbErrorMessage(
                aq2rdbResponse, 400,
                'If \"b\" is specified, a valid 14-digit ISO ' +
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
            '\"n\", \"d\", and \"y\" fields ' +
                'must be present when \"t\" is \"MEAS\"'
        );

        // TODO: provide URL to aq2rdb documentation instead?
        // nwrt2rdb_usage();

        // TODO: map to HTTP error code?
        // status = 124;
        return;
    }

    // set expand rating flag as a character
    // TODO: "-e" was maybe an "overloaded" option in
    // nwts2rdb? Need to find out if this is still
    // relevant.
    /*
      if (eflag) {
      strcpy (expandit,'Y');
      }
      else {
      strcpy (expandit,'N');
      }
    */

    // if time-series identifier is present
    if (field.timeSeriesIdentifier !== undefined) {
        // see
        // https://sites.google.com/a/usgs.gov/aquarius-api-wiki/publish/gettimeseriesdescriptionlist
        getTimeSeriesDescriptionList(field, aq2rdbResponse);

        // Use GetTimeSeriesRawDaa [sic] or getTimeSeriesCorrectedData
        // with the TimeSeriesUniqueId parameter to get the data.
        getTimeSeriesCorrectedData(field, aq2rdbResponse);
    }

    // if time-series identifier is not present, and location
    // identifier is present
    if (field.timeSeriesIdentifier === undefined &&
        field.locationIdentifier !== undefined) {
        getTimeSeriesDescriptionList(field, aq2rdbResponse);
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
    request.on('error', function(error) {
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
