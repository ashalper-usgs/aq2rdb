/**
 * @fileOverview A Web service to map AQUARIUS, time series data
 *               requests to USGS-variant RDB files.
 *
 * @author <a href="mailto:ashalper@usgs.gov">Andrew Halper</a>
 *
 * @see <a href="https://sites.google.com/a/usgs.gov/nwis_integrator/data_retrieval/cli/aqts2rdb">aqts2rdb</a>.
 */

'use strict';
var serviceName = 'aq2rdb';
var http = require('http');
var httpdispatcher = require('httpdispatcher');
var querystring = require('querystring');
var syncRequest = require('sync-request'); // make synchronous HTTP requests

/**
   @description The port the aq2rdb service listens on.
*/
var PORT = 8081;

/**
   @description HTTP query parameter existence and check for non-empty
                parameter content.
*/
function getParameter(parameterName, parameterValue, description, response) {
    var statusMessage;

    if (parameterValue === undefined) {
        // "Bad Request" HTTP status code
        statusMessage = 'Required parameter \"' + parameterName +
            '\" (' + description + ') not present';
        console.log(serviceName + ': ' + statusMessage);
        response.writeHead(400, statusMessage,
                           {'Content-Length': statusMessage.length,
                            'Content-Type': 'text/plain'});
        response.end(statusMessage);
    }
    else if (parameterValue.trim() === '') {
        // "Bad Request" HTTP status code
        statusMessage = 'No content in parameter \"' + parameterName +
            '\" (' + description + ')';
        console.log(serviceName + ': ' + statusMessage);
        response.writeHead(400, statusMessage,
                           {'Content-Length': statusMessage.length,
                            'Content-Type': 'text/plain'});
        response.end(statusMessage);
    }
    return parameterValue;
}

function unknownError(response, message) {
    console.log(serviceName + ': ' + message);
    response.writeHead(500, message,
                       {'Content-Length': message.length,
                        'Content-Type': 'text/plain'});
    response.end(message);
}

// GET request handler
httpdispatcher.onGet('/' + serviceName, function (request, response) {
    var getAQTokenHostname = 'localhost';     // GetAQToken service host name
    var aquariusHostname = 'nwists.usgs.gov'; // AQUARIUS host name
    // parse HTTP query parameters in GET request URL
    var arg = querystring.parse(request.url);
    var username =
        getParameter('Username', arg.Username, 'AQUARIUS user name',
                     response);
    var password =
        getParameter('Password', arg.Password, 'AQUARIUS password',
                     response);
    var z = getParameter('z', arg.z, 'AQUARIUS environment',
                         response);
    var t = getParameter('t', arg.t, 'data type', response);
    var n = getParameter('n', arg.n, 'site number', response);

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
        response.writeHead(400, statusMessage,
                           {'Content-Length': statusMessage.length,
                            'Content-Type': 'text/plain'});
        response.end(statusMessage);
    }

    var getAQTokenResponse;
    try {
        // send (synchronous) request to GetAQToken service for AQUARIUS
        // authentication token
        getAQTokenResponse =
            syncRequest(
                'GET',
                'http://' + getAQTokenHostname +
                    ':8080/services/GetAQToken?&userName=' +
                    username + '&password=' + password +
                    '&uriString=http://nwists.usgs.gov/AQUARIUS/'
            );
    }
    catch (error) {
        unknownError(response, error.message);
    }

    var token = getAQTokenResponse.getBody().toString('utf-8');

    var aquariusResponse;
    try {
        // send (synchronous) request to GetAQToken service for AQUARIUS
        // authentication token
        aquariusResponse =
            syncRequest(
                'GET',
                'http://' + aquariusHostname +
                    '/AQUARIUS/Publish/V2/' +
                    'getTimeSeriesDescriptionList?' +
                    '&token=' + token + '&format=json' +
                    '&locationIdentifier=' + locationIdentifier
            );
    }
    catch (error) {
        unknownError(response, error.message);
    }

    // TODO: move to callback
    // response.writeHead(200, {'Content-Type': 'text/plain'});

    // TODO: RDB output goes here
    response.end(token);
});

function handleRequest(request, response) {
    try {
        httpdispatcher.dispatch(request, response);
    } catch (error) {
        var statusMessage;

        if (error.message === 'connect ECONNREFUSED') {
            statusMessage = 'could not connect to AQUARIUS';
            console.log(serviceName + ': ' + statusMessage);
            response.writeHead(504, statusMessage,
                               {'Content-Length': statusMessage.length,
                                'Content-Type': 'text/plain'});
        }
        else {
            unknownError(response, error.message);
        }
        response.end(statusMessage);
    }
}

var server = http.createServer(handleRequest);

server.listen(PORT, function () {
    console.log('Server listening on: http://localhost:%s', PORT);
});
