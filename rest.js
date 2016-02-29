/**
 * @fileOverview Functions to facilitate REST HTTP queries.
 *
 * @author <a href="mailto:ashalper@usgs.gov">Andrew Halper</a>
 */

'use strict';

// Node.js modules
var http = require('http');
var https = require('https');
var querystring = require('querystring');

var rest = module.exports = {
    /**
       @function Call a REST Web service with an HTTP query; send
                 response via a callback.
       @private
       @param {string} host Host part of HTTP query URL.
       @param {string} method HTTP request method (e.g. "GET", "POST").
       @param {object} HTTP requst headers.
       @param {string} path Path part of HTTP query URL.
       @param {object} field An array of attribute-value pairs to bind in
              HTTP query URL.
       @param {function} callback Callback function to call if/when
              response from Web service is received.
    */
    query: function (host, method, headers, path, obj, log, callback) {
        /**
           @description Handle response from HTTP query.
           @callback
        */
        function queryCallback(response) {
            var messageBody = '';

            // accumulate response
            response.on(
                'data',
                function (chunk) {
                    messageBody += chunk;
                });

            response.on('end', function () {
                if (log)
                    console.log("rest.query.response.statusCode: " +
                                response.statusCode.toString());
                if (response.statusCode === 404) {
                    callback("Site not found at http://" + host + path);
                }
                else if (
                    response.statusCode < 200 || 300 <= response.statuscode
                )
                    callback(
                        "Could not reference site at http://" + host +
                            path + "; HTTP status code was: " +
                            response.statusCode.toString()
                    );
                else
                    callback(null, messageBody);
                return;
            });
        }

        if (method === "GET" && obj !== undefined)
            path += '?' + querystring.stringify(obj);

        if (method === "POST")
            var chunk = querystring.stringify(obj);

        if (log)
            console.log("rest.query: http://" + host + path);

        var request = http.request({
            host: host,
            method: method,
            headers: headers,
            path: path
        }, queryCallback);

        /**
           @description Handle service invocation errors.
        */
        request.on("error", function (error) {
            if (log)
                console.log("rest.query: " + error);
            callback(error);
            return;
        });

        if (method === "POST")
            request.write(chunk);

        request.end();
    }, // query

    /**
       @function Call a REST Web service with an HTTPS query; send
                 response via a callback.
       @private
       @param {string} host Host part of HTTP query URL.
       @param {string} method HTTP request method (e.g. "GET", "POST").
       @param {object} HTTP requst headers.
       @param {string} path Path part of HTTP query URL.
       @param {object} field An array of attribute-value pairs to bind in
              HTTP query URL.
       @param {function} callback Callback function to call if/when
              response from Web service is received.
    */
    querySecure: function (host, method, headers, path, obj, log, callback) {
        /**
           @description Handle response from HTTPS query.
           @callback
        */
        function queryCallback(response) {
            var messageBody = '';

            // accumulate response
            response.on(
                'data',
                function (chunk) {
                    messageBody += chunk;
                });

            response.on('end', function () {
                if (log)
                    console.log("rest.query.response.statusCode: " +
                                response.statusCode.toString());
                if (response.statusCode === 404) {
                    callback("Site not found at http://" + host + path);
                }
                else if (
                    response.statusCode < 200 || 300 <= response.statuscode
                )
                    callback(
                        "Could not reference site at http://" + host +
                            path + "; HTTP status code was: " +
                            response.statusCode.toString()
                    );
                else
                    callback(null, messageBody);
                return;
            });
        }

        if (method === "GET" && obj !== undefined)
            path += '?' + querystring.stringify(obj);

        if (method === "POST")
            var chunk = querystring.stringify(obj);

        if (log)
            console.log("rest.query: https://" + host + path);

        var request = https.request({
            host: host,
            method: method,
            headers: headers,
            path: path
        }, queryCallback);

        /**
           @description Handle service invocation errors.
        */
        request.on("error", function (error) {
            if (log)
                console.log("rest.query: " + error);
            callback(error);
            return;
        });

        if (method === "POST")
            request.write(chunk);

        request.end();
    } // querySecure

} // rest
