/**
 * @fileOverview Test stub Web service to impersonate GetAQToken and
 *               AQUARIUS Web services when they are unavailable.
 *
 * @author <a href="mailto:ashalper@usgs.gov">Andrew Halper</a>
 */

'use strict';
var http = require('http');

/**
   @description The port for impersonating GetAQToken.
*/
var GETAQTOKEN_PORT = 8080;

/**
   @description Service dispatcher.
*/ 
function handleRequest(request, response) {
    try {
        console.log('request: ' + JSON.stringify(request));
    }
    catch (error) {
        throw error;
    }
}

/**
   @description Create HTTP server to host the service.
*/ 
var server = http.createServer(handleRequest);

/**
   @description Start listening for requests.
*/
server.listen(GETAQTOKEN_PORT, function () {
    console.log('Server listening on: http://localhost:' +
		GETAQTOKEN_PORT.toString());
});
