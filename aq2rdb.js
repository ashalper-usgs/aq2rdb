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
   @description HTTP query parameter existence and check for non-empty
                parameter content.
*/
function getParameter(
    parameterName, parameterValue, description, response
) {
    var statusMessage;

    if (parameterValue === undefined) {
        // "Bad Request" HTTP status code
        statusMessage = 'Required parameter \"' + parameterName +
            '\" (' + description + ') not present';
        console.log(SERVICE_NAME + ': ' + statusMessage);
        response.writeHead(400, statusMessage,
                           {'Content-Length': statusMessage.length,
                            'Content-Type': 'text/plain'});
        response.end(statusMessage);
    }
    else if (parameterValue.trim() === '') {
        // "Bad Request" HTTP status code
        statusMessage = 'No content in parameter \"' + parameterName +
            '\" (' + description + ')';
        console.log(SERVICE_NAME + ': ' + statusMessage);
        response.writeHead(400, statusMessage,
                           {'Content-Length': statusMessage.length,
                            'Content-Type': 'text/plain'});
        response.end(statusMessage);
    }
    return parameterValue;
}

/**
   @description Process unforeseen errors.
*/
function unknownError(response, message) {
    response.writeHead(500, message,
                       {'Content-Length': message.length,
                        'Content-Type': 'text/plain'});
    response.end(message);
}

/**
   @description Service GET request handler.
*/ 
httpdispatcher.onGet('/' + SERVICE_NAME,
                     function (aq2rdbRequest, aq2rdbResponse) {
    var getAQTokenHostname = 'localhost';     // GetAQToken service host name
    // parse HTTP query parameters in GET request URL
    var arg = querystring.parse(aq2rdbRequest.url);
    var username =
        getParameter('Username', arg.Username, 'AQUARIUS user name',
                     aq2rdbResponse);
    var password =
        getParameter('Password', arg.Password, 'AQUARIUS password',
                     aq2rdbResponse);
    var z = getParameter('z', arg.z, 'AQUARIUS environment',
                         aq2rdbResponse);
    var t = getParameter('t', arg.t, 'data type', aq2rdbResponse);
    var n = getParameter('n', arg.n, 'site number', aq2rdbResponse);

    // default environment ("z") parameter value
    if (z === undefined) {
        z = 'production';
    }

    // default agency code to "USGS" if not present
    var locationIdentifier =
        arg.a === undefined ? n + '-USGS' : n + '-' + arg.a;

    // data type ("t") parameter domain validation
    var statusMessage;
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

    if (statusMessage != undefined) {
        // there was an error
        aq2rdbResponse.writeHead(400, statusMessage,
                           {'Content-Length': statusMessage.length,
                            'Content-Type': 'text/plain'});
        aq2rdbResponse.end(statusMessage);
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
           @description Buffer response from AQUARIUS API.
        */
        function getTimeSeriesDescriptionListCallback(
            getTimeSeriesDescriptionListResponse
        ) {
            var messageBody = '';

            // accumulate response
            getTimeSeriesDescriptionListResponse.on(
                'data',
                function (chunk) {
                    messageBody += chunk;
                });

            getTimeSeriesDescriptionListResponse.on('end', function () {
                console.log(
                    SERVICE_NAME +
                        ': getTimeSeriesDescriptionList request ' +
                        'complete; messageBody: ' + messageBody
                );
                console.log(
                    SERVICE_NAME +
                        ': getTimeSeriesDescriptionListResponse.statusCode: ' +
                        getTimeSeriesDescriptionListResponse.statusCode
                );
                var aquarius = JSON.parse(messageBody);
                console.log(
                    SERVICE_NAME +
                        '.aquarius.ResponseStatus.ErrorCode: ' +
                        aquarius.ResponseStatus.ErrorCode
                );
                if (getTimeSeriesDescriptionListResponse.statusCode === 400) {
                var statusMessage =
                '# There was a problem forwarding the request to AQUARIUS:\n' +
                '#\n' +
                '#   ' + aquarius.ResponseStatus.Message + '\n';
                    aq2rdbResponse.writeHead(
                        getTimeSeriesDescriptionListResponse.statusCode,
                        statusMessage,
                        {'Content-Length': statusMessage.length,
                         'Content-Type': 'text/plain'}
                    );
                    aq2rdbResponse.end(statusMessage);
                }
            });
        } // getTimeSeriesDescriptionListCallback

        http.request({
            host: AQUARIUS_HOSTNAME,
            path: '/AQUARIUS/Publish/V2/' +
                'getTimeSeriesDescriptionList?' +
                '&token=' + token + '&format=json' +
                '&locationIdentifier=' + locationIdentifier
        }, getTimeSeriesDescriptionListCallback).end();
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
            // now interrogate AQUARIUS API for water data
            // TODO: more parameters need to be passed here?
            getAquariusResponse(token, locationIdentifier);
        });
    } // getAQTokenCallback

    // GetAQToken service request for AQUARIUS authentication token
    // needed for AQUARIUS API
    http.request({
        host: getAQTokenHostname,
        port: '8080',
        path: '/services/GetAQToken?&userName=' +
            username + '&password=' + password +
            '&uriString=http://' + AQUARIUS_HOSTNAME + '/AQUARIUS/'
    }, getAQTokenCallback).end();

    // TODO: move to callback
    // aq2rdbResponse.writeHead(200, {'Content-Type': 'text/plain'});

    // TODO: need to somehow pass error messages down here to pass to
    // aq2rdbResponse.end().
});

/**
   @description Service dispatcher (there is only one path to
                dispatch).
*/ 
function handleRequest(request, response) {
    try {
        httpdispatcher.dispatch(request, response);
    }
    catch (error) {
        // TODO: this is greedily catching all errors in
        // httpdispatcher.onGet() above, which is not what we want
        // (e.g. when AQUARIUS replies with a HTTP 400 error when a
        // site does not exist in the database).

        var statusMessage;

        if (error.message === 'connect ECONNREFUSED') {
            statusMessage = 'could not connect to AQUARIUS';
            console.log(SERVICE_NAME + ': ' + statusMessage);
            response.writeHead(504, statusMessage,
                               {'Content-Length': statusMessage.length,
                                'Content-Type': 'text/plain'});
        }
        else if (error.statusCode === 400) {
            console.log(
                'handleRequest::error.statusCode: ' +
                    error.statusCode.toString()
            );
            console.log('error.message: ' + error.message);
        }
        else {
            unknownError(response, error.message);
        }
        response.end(statusMessage);
    }
}

/**
   @description Create HTTP server.
*/ 
var server = http.createServer(handleRequest);

/**
   @description Start listening for requests.
*/ 
server.listen(PORT, function () {
    console.log('Server listening on: http://localhost:%s', PORT);
});
