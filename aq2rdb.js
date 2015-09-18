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
function getParameter(parameterName, parameterValue, description, response) {
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
    console.log('unknownError(');
    console.log('  request: ' + response);
    console.log('  message: ' + message);
    console.log(')');

    console.log(SERVICE_NAME + ': ' + message);
    response.writeHead(500, message,
                       {'Content-Length': message.length,
                        'Content-Type': 'text/plain'});
    response.end(message);
}

function aquariusRequest(token, locationIdentifier) {
    console.log(SERVICE_NAME + '.aquariusRequest().token: ' +
                token);
    console.log(
        SERVICE_NAME +
            ': Sending AQUARIUS getTimeSeriesDescriptionList ' +
            'request...'
    );

    function getTimeSeriesDescriptionListCallback(response) {
        var data = '';

        // accumulate response
        response.on('data', function (chunk) {
            data += chunk;
        });

        response.on('end', function () {
            console.log(
                SERVICE_NAME +
                    ': getTimeSeriesDescriptionList request ' +
                    'complete; data: ' + data
            );
        });
    } // getTimeSeriesDescriptionListCallback

    console.log('AQUARIUS_HOSTNAME: ' + AQUARIUS_HOSTNAME);
    console.log('token: ' + token);
    console.log('locationIdentifier: ' + locationIdentifier);

    http.request({
        host: AQUARIUS_HOSTNAME,
        path: '/AQUARIUS/Publish/V2/' +
            'getTimeSeriesDescriptionList?' +
            '&token=' + token + '&format=json' +
            '&locationIdentifier=' + locationIdentifier
    }, getTimeSeriesDescriptionListCallback).end();
}

/**
   @description Service GET request handler.
*/ 
httpdispatcher.onGet('/' + SERVICE_NAME, function (request, response) {
    console.log('httpdispatcher.onGet(');
    console.log('  request: ' + request);
    console.log('  response: ' + response);
    console.log(')');

    var getAQTokenHostname = 'localhost';     // GetAQToken service host name
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
            // now interrogate AQUARIUS API for water data
            // TODO: more parameters need to be passed here?
            aquariusRequest(token, locationIdentifier);
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
    // response.writeHead(200, {'Content-Type': 'text/plain'});

});

/**
   @description Service dispatcher (there is only one path to
                dispatch).
*/ 
function handleRequest(request, response) {
    console.log('handleRequest(');
    console.log('  request: ' + request);
    console.log('  response: ' + response);
    console.log(')');
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
