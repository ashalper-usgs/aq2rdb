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
function aq2rdbErrorMessage(response, statusCode, statusMessage) {
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
            aq2rdbResponse.end(JSON.stringify(timeSeriesDescriptions[1]));
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

function getTimeSeriesCorrectedData(path, aq2rdbResponse) {
    log(': path: ' + path);
    /**
       @description Handle response from
                    TimeSeriesDataCorrectedServiceRequest.
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
            var timeSeriesDataCorrected = JSON.parse(messageBody);
            aq2rdbResponse.end(JSON.stringify(timeSeriesDataCorrected));
        });
    } // callback

    var request = http.request({
        host: AQUARIUS_HOSTNAME,
        path: path
    }, callback);

    /**
       @description Handle GetTimeSeriesDescriptionList service
                    invocation errors.
    */
    request.on('error', function (error) {
        handleService('TimeSeriesDataCorrectedServiceRequest',
                      aq2rdbResponse, error);
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
function aquariusDispatch(field, aq2rdbResponse) {
    // if time-series identifier is present
    if (field.timeSeriesIdentifier !== undefined) {
        // On Wed, Sep 23, 2015 at 1:24 PM, Scott Bartholoma said:
        // 
        // Parse out the Parameter (the part before the first "." and
        // the location identifier (the part after the "@") form the
        // identifier string

        // TODO: need error handling here
        field.parameter = field.timeSeriesIdentifier.split('.')[0];
        field.locationIdentifier = field.timeSeriesIdentifier.split('@')[1];

        // Use GetTimeSeriesDescriptionList with the
        // LocationIdentifier and Parameter parameters in the URL and
        // then find the requested timeseries in the output to get tue
        // GUID

        // see
        // https://sites.google.com/a/usgs.gov/aquarius-api-wiki/publish/gettimeseriesdescriptionlist
        getTimeSeriesDescriptionList(field, aq2rdbResponse);

        // Use GetTimeSeriesRawDaa [sic] or getTimeSeriesCorrectedData
        // with the TimeSeriesUniqueId parameter to get the data.

        // TODO: instead of building URL paths here, consider passing
        // (the rather ephemeral) Web service parameters as a
        // JavaScript object.
        var path = '/AQUARIUS/Publish/V2/' +
            'GetTimeSeriesCorrectedData?' +
            'token=' + field.token + '&format=json' +
            '&TimeSeriesUniqueId=' +
            '8b1d6f626f63470cb12631027b60479e' +
            bind('QueryFrom', field.queryFrom) +
            bind('QueryTo', field.QueryTo);
        getTimeSeriesCorrectedData(path, aq2rdbResponse);
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
    log('httpdispatcher.onGet()');
    // parse HTTP query parameters in GET request URL
    var arg = querystring.parse(aq2rdbRequest.url);
    var field = new Object();
    var statusMessage;

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

    // AQUARIUS "environment"
    if (arg.z.length === 0) {
        z = 'production';       // set default
    }

    // data type
    if (arg.t.length === 0) {
        aq2rdbErrorMessage(
            aq2rdbResponse, 400,
            '# ' + SERVICE_NAME + ': Required parameter ' +
                '\"t\" (data type) not found'
        );
        return;
    }
    var t = arg.t;

    var n = arg.n;

    // "time-series identifier"
    field.timeSeriesIdentifier = arg.u;

    // AQUARIUS does ill-advised things with site PK to accomodate
    // programmer lazyness
    field.locationIdentifier =
        arg.a === undefined ? n : n + '-' + arg.a;

    field.queryFrom = arg.b;
    field.queryTo = arg.e;

    // data type ("t") parameter domain validation
    switch (t.toLowerCase()) {
    case 'ms':
        statusMessage =
            'Pseudo-time series (e.g., gage inspections) are not supported';
        break;
    case 'vt':
        statusMessage = 'Sensor inspections and readings are not supported';
        break;
    case 'pk':
        statusMessage = 'Peak-flow data are not supported';
        break;
    case 'dc':
        statusMessage = 'Data corrections are not supported';
        break;
    case 'sv':
        statusMessage = 'Quantitative site-visit data are not supported';
        break;
    case 'wl':
        statusMessage = 'Discrete groundwater-levels data are not supported';
        break;
    case 'qw':
        statusMessage = 'Discrete water quality data are not supported';
        break;
    // these are the only valid "t" parameter values right now
    case 'dv':
    case 'uv':
        break;
    default:
        statusMessage =
            'Unknown \"t\" (data type) parameter value: \"' + t +
            '\"';
    }

    if (statusMessage !== undefined) {
        // there was an error
        aq2rdbErrorMessage(aq2rdbResponse, statusCode, statusMessage);
    }

    /**
       @description Store AQUARIUS Web service API authentication
                    token.
    */
    var token = '';

    /**
       @description GetAQToken service response callback.
    */
    function getAQTokenCallback(response) {
        // accumulate response
        response.on('data', function (chunk) {
            token += chunk;
        });

        // response complete
        response.on('end', function () {
            field.token = token;
            aquariusDispatch(field, aq2rdbResponse);
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
            log('361: error.message: ' + error.message);
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
        log('379: error.message: ' + error.message);
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
