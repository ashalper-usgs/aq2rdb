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
   @description Process unforeseen errors.
*/
function unknownError(response, message) {
    console.log(SERVICE_NAME + ': unknownError()');
    response.writeHead(500, message,
                       {'Content-Length': message.length,
                        'Content-Type': 'text/plain'});
    response.end(message);
}

/**
   @description Consolodated error message writer.
*/ 
function writeErrorMessage(response, statusCode, statusMessage) {
    response.writeHead(statusCode, statusMessage,
                       {'Content-Length': statusMessage.length,
                        'Content-Type': 'text/plain'});
    response.end(statusMessage);
}

/**
   @description Retreive time series data from AQUARIUS API.
*/
function getTimeSeriesDescriptionList(
    token, locationIdentifier, aq2rdbResponse
) {
    /**
       @description Handle response from getTimeSeriesDescriptionList
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
            var statusMessage = '';

            switch (response.statusCode) {
            case 301:
                statusMessage =
                    '# There was a problem forwarding the data ' +
                    'request to AQUARIUS. The message was:\n' +
                    '#\n' +
                    '#   ' + response.statusMessage;
                // write error as a plain text RDB comment
                aq2rdbResponse.writeHead(
                    response.statusCode,
                    statusMessage,
                    {'Content-Length': statusMessage.length,
                     'Content-Type': 'text/plain'}
                );
                aq2rdbResponse.end(statusMessage);
                return;
            case 400:
                // TODO: it is unlikely, but there could be errors
                // in parsing the JSON reply here, so this needs
                // its own error handling
                var messageObject = JSON.parse(messageBody);
                // TODO: this could probably be made more
                // robust/helpful. Right now the message is just
                // the vague "Unable to bind request".
                statusMessage =
                    '# There was a problem forwarding the data ' +
                    'request to AQUARIUS. The message was:\n' +
                    '#\n' +
                    '#   ' + messageObject.ResponseStatus.Message;
                // write error as a plain text RDB comment
                aq2rdbResponse.writeHead(
                    response.statusCode,
                    statusMessage,
                    {'Content-Length': statusMessage.length,
                     'Content-Type': 'text/plain'}
                );
                aq2rdbResponse.end(statusMessage);
                return;
            case 401:
                // TODO: "401: Unauthorized"
                return;
            }
            aq2rdbResponse.end(messageBody);
        });
    } // callback

    var request =
        http.request({
            host: AQUARIUS_HOSTNAME,
            path:
                'AQUARIUS/Publish/V2/getTimeSeriesDescriptionList?' +
                '&token=' + token + '&format=json' +
                '&Parameter=Discharge&ExtendedFilters=' +
                '[{FilterName:ACTIVE_FLAG,FilterValue:Y}]' +
                '&locationIdentifier=' + locationIdentifier
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

/**
   @description Service GET request handler.
*/ 
httpdispatcher.onGet('/' + SERVICE_NAME, function (
    aq2rdbRequest, aq2rdbResponse
) {
    var getAQTokenHostname = 'localhost';     // GetAQToken service host name
    // parse HTTP query parameters in GET request URL
    var arg = querystring.parse(aq2rdbRequest.url);
    var statusMessage;

    if (arg.Username.length === 0) {
        writeErrorMessage(
            aq2rdbResponse, 400,
            '# ' + SERVICE_NAME + ': Required parameter ' +
                '\"Username\" not found'
        );
        return;
    }
    var username = arg.Username;

    if (arg.Password.length === 0) {
        writeErrorMessage(
            aq2rdbResponse, 400,
            '# ' + SERVICE_NAME + ': Required parameter ' +
                '\"Password\" not found'
        );
        return;
    }
    var password = arg.Password;

    // AQUARIUS "environment"
    if (arg.z.length === 0) {
        z = 'production';       // set default
    }

    // data type
    if (arg.t.length === 0) {
        writeErrorMessage(
            aq2rdbResponse, 400,
            '# ' + SERVICE_NAME + ': Required parameter ' +
                '\"t\" (data type) not found'
        );
        return;
    }
    var t = arg.t;

    var n = arg.n;

    // time-series identifier
    var u = arg.u;

    // AQUARIUS does ill-advised things with site PK to accomodate
    // programmer lazyness
    var locationIdentifier =
        arg.a === undefined ? n : n + '-' + arg.a;

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
        writeErrorMessage(aq2rdbResponse, statusCode, statusMessage);
    }

    /**
       @description Store AQUARIUS Web service API authentication
                    token.
    */
    var token = '';

    /**
       @description Retreive time series data from AQUARIUS API.
    */
    function getAquariusResponse(token, locationIdentifier) {
        /**
           @description Handle response from AQUARIUS API.
        */
        function getTimeSeriesCorrectedDataCallback(
            getTimeSeriesCorrectedDataResponse
        ) {
            var messageBody = '';

            // accumulate response
            getTimeSeriesCorrectedDataResponse.on(
                'data',
                function (chunk) {
                    messageBody += chunk;
                });

            getTimeSeriesCorrectedDataResponse.on('end', function () {
                switch (getTimeSeriesCorrectedDataResponse.statusCode) {
                case 400:
                    // TODO: it is unlikely, but there could be errors
                    // in parsing the JSON reply here, so this needs
                    // its own error handling
                    var messageObject = JSON.parse(messageBody);
                    // TODO: this could probably be made more
                    // robust/helpful. Right now the message is just
                    // the vague "Unable to bind request".
                    statusMessage =
                        '# There was a problem forwarding the ' +
                        'request to AQUARIUS:\n' +
                        '#\n' +
                        '#   ' + messageObject.ResponseStatus.Message;
                    // write error as a plain text RDB comment
                    aq2rdbResponse.writeHead(
                        getTimeSeriesCorrectedDataResponse.statusCode,
                        statusMessage,
                        {'Content-Length': statusMessage.length,
                         'Content-Type': 'text/plain'}
                    );
                    aq2rdbResponse.end(statusMessage);
                    return;
                case 401:
                    // TODO: "401: Unauthorized"
                    return;
                }
            });
        } // getTimeSeriesCorrectedDataCallback

        // see
        // http://nwists.usgs.gov/AQUARIUS/Publish/v2/json/metadata?op=TimeSeriesDataCorrectedServiceRequest
        var request =
            http.request({
                host: AQUARIUS_HOSTNAME,
                path: '/AQUARIUS/Publish/V2/' +
                    'GetTimeSeriesCorrectedData?' +
                    '&token=' + token + '&format=json' +
                    '&TimeSeriesUniqueId=' + u
            }, getTimeSeriesCorrectedDataCallback);

        /**
           @description Handle GetTimeSeriesCorrectedData service
                        invocation errors.
        */
        request.on('error', function (error) {
            handleService('GetTimeSeriesCorrectedData',
                          aq2rdbResponse, error);
        });

        request.end();
    } // getAquariusResponse

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
            // TODO: analyze aq2rdb parameters and dispatch to
            // appropriate AQUARIUS API call here?

            // if time-series identifier not present, and location
            // identifier is present
            if (u === undefined && locationIdentifier !== undefined) {
                getTimeSeriesDescriptionList(
                    token, locationIdentifier, aq2rdbResponse
                );
            }

            // now interrogate AQUARIUS API for water data
            // TODO: more parameters need to be passed here?
            // getAquariusResponse(token, locationIdentifier);
        });
    } // getAQTokenCallback

    /**
       @description GetAQToken service request for AQUARIUS
                    authentication token needed for AQUARIUS API.
    */
    var request =
        http.request({
            host: getAQTokenHostname,
            port: '8080',
            path: '/services/GetAQToken?&userName=' +
                username + '&password=' + password +
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
            unknownError(aq2rdbResponse, error.message);
        }
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
        unknownError(response, error.message);
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
    console.log('Server listening on: http://localhost:%s', PORT);
});
