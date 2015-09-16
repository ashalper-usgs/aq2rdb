'use strict';
var name = 'aq2rdb';
var http = require('http');
var httpdispatcher = require('httpdispatcher');
var querystring = require('querystring');
var syncRequest = require('sync-request'); // make synchronous HTTP requests

var PORT = 8081;

function handleRequest(request, response) {
    try {
        console.log(request.url);
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
            response.writeHead(500, error.message,
                               {'Content-Length': error.message.length,
                                'Content-Type': 'text/plain'});
            console.log(name + ': ' + error.toString());
        }
        response.end();
    }
}

// HTTP query parameter existence and non-empty content validation
function getParameter(parameter, response) {
    var statusMessage;

    if (parameter === undefined) {
        // "Bad Request" HTTP status code
        statusMessage = 'Required parameter not present';
        response.writeHead(400, statusMessage,
                           {'Content-Length': statusMessage.length,
                            'Content-Type': 'text/plain'});
        response.end();
    }
    else if (parameter.trim() === '') {
        // "Bad Request" HTTP status code
        statusMessage = 'No content in parameter';
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

    // data type ("t") parameter domain validation
    var statusMessage;
    switch (t) {
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
    default:
        statusMessage = 'Unknown \"t\" parameter value: \"' + t + '\"';
    }

    if (statusMessage != undefined) {
        response.writeHead(400, statusMessage,
                           {'Content-Length': statusMessage.length,
                            'Content-Type': 'text/plain'});
        response.end('Bad Request');
    }

    console.log('Username: ' + username);
    console.log('Password: ' + password);
    console.log('z: ' + z);
    console.log('t: ' + t);

    // send (synchronous) request to GetAQToken service for AQUARIUS
    // authentication token
    var getAQTokenResponse =
        syncRequest(
            'GET',
            'http://localhost:8080/services/GetAQToken?&userName=' +
                username + '&password=' + password +
                '&uriString=http://nwists.usgs.gov/AQUARIUS/'
        );

    // TODO: need to handle AQUARIUS server GET response errors before
    // here

    // var token = getAQTokenResponse.getBody().toString('utf-8');

    // TODO: AQUARIUS Web service request and callback goes here

    // TODO: move to callback
    // response.writeHead(200, {'Content-Type': 'text/plain'});

    // TODO: RDB output goes here
    response.end('aq2rdb');
});

var server = http.createServer(handleRequest);

server.listen(PORT, function () {
    console.log('Server listening on: http://localhost:%s', PORT);
});
