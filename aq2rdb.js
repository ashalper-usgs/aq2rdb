'use strict';
var name = 'aq2rdb';
var http = require('http');
var httpdispatcher = require('httpdispatcher');
var querystring = require('querystring');
var syncRequest = require('sync-request'); // make synchronous HTTP requests

var PORT = 8081;

// HTTP query parameter existence and non-empty content validation
function getParameter(parameter, response) {
    var statusMessage;

    if (parameter === undefined) {
        // "Bad Request" HTTP status code
        statusMessage = 'Required parameter not present';
        console.log(name + ': ' + statusMessage);
        response.writeHead(400, statusMessage,
                           {'Content-Length': statusMessage.length,
                            'Content-Type': 'text/plain'});
        response.end();
    }
    else if (parameter.trim() === '') {
        // "Bad Request" HTTP status code
        statusMessage = 'No content in parameter';
        console.log(name + ': ' + statusMessage);
        response.writeHead(400, statusMessage,
                           {'Content-Length': statusMessage.length,
                            'Content-Type': 'text/plain'});
        response.end();
    }
    return parameter;
}

// GET request handler
httpdispatcher.onGet('/' + name, function (request, response) {
    // parse HTTP query parameters in GET request URL
    var arg = querystring.parse(request.url);
    var username = getParameter(arg.Username, response);
    var password = getParameter(arg.Password, response);
    var z = getParameter(arg.z, response);
    var t = getParameter(arg.t, response);

    // default environment ("z") parameter
    if (z === undefined) {
	z = 'production';
    }

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
    case 'dv':
    case 'uv':
        // the only valid "t" parameter values right now
        break;
    default:
        statusMessage = 'Unknown \"t\" parameter value: \"' + t + '\"';
    }

    var getAQTokenResponse;
    if (statusMessage === undefined) {
        // send (synchronous) request to GetAQToken service for AQUARIUS
        // authentication token
        getAQTokenResponse =
            syncRequest(
                'GET',
                'http://localhost:8080/services/GetAQToken?&userName=' +
                    username + '&password=' + password +
                    '&uriString=http://nwists.usgs.gov/AQUARIUS/'
            );
    }
    else {
        // there was an error
        response.writeHead(400, statusMessage,
                           {'Content-Length': statusMessage.length,
                            'Content-Type': 'text/plain'});
        response.end();
    }

    // TODO: need to handle AQUARIUS server GET response errors before
    // here

    // var token = getAQTokenResponse.getBody().toString('utf-8');

    // TODO: AQUARIUS Web service request and callback goes here

    // TODO: move to callback
    // response.writeHead(200, {'Content-Type': 'text/plain'});

    // TODO: RDB output goes here
    response.end(getAQTokenResponse.getBody().toString('utf-8'));
});

function handleRequest(request, response) {
    try {
        httpdispatcher.dispatch(request, response);
    } catch (error) {
        var statusMessage;

        if (error.message === 'connect ECONNREFUSED') {
            statusMessage = 'could not connect to AQUARIUS';
            console.log(name + ': ' + statusMessage);
            response.writeHead(504, statusMessage,
                               {'Content-Length': statusMessage.length,
                                'Content-Type': 'text/plain'});
        }
        else {
            // unknown error
            console.log(name + ': ' + error.message);
            response.writeHead(500, error.message,
                               {'Content-Length': error.message.length,
                                'Content-Type': 'text/plain'});
        }
        response.end();
    }
}

var server = http.createServer(handleRequest);

server.listen(PORT, function () {
    console.log('Server listening on: http://localhost:%s', PORT);
});
