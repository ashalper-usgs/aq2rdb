/**
 * @fileOverview Functions to facilitate REST HTTP queries.
 *
 * @module rest
 *
 * @author Andrew Halper <ashalper@usgs.gov>
 */

'use strict';

// Node.js modules
var http = require('http');
var https = require('https');
var querystring = require('querystring');

var rest = module.exports = {
    /**
       @see http://www.tomas-dvorak.cz/posts/nodejs-request-without-dependencies/
    */
    query: function (protocol, host, method, headers, path, obj, log) {
        // return new pending promise
        return new Promise((resolve, reject) => {
            // select http or https module, depending on reqested url
            const lib =
                (protocol === 'https') ? require('https') : require('http');

            if (method === "GET" && obj !== undefined)
                path += '?' + querystring.stringify(obj);

            if (method === "POST")
                var chunk = querystring.stringify(obj);

            if (false)
                console.log(protocol + "://" + host + path);

            const request = lib.get({
                host: host,
                method: method,
                headers: headers,
                path: path
            }, (response) => {
                // handle HTTP errors
                if (response.statusCode < 200 || 299 < response.statusCode) {
                    /**
                       @todo might need to change reject()'s argument
                             here to make error handling more
                             sophisticated.
                    */
                    reject(response.statusCode);
                }
                // temporary data holder
                const body = [];
                // on every content chunk, push it to the data array
                response.on('data', (chunk) => body.push(chunk));
                // we are done, resolve promise with those joined chunks
                response.on('end', () => resolve(body.join('')));
            });
            // handle connection errors of the request
            request.on('error', (err) => reject(err));

            // post query if POST request
            if (method === "POST")
                request.write(chunk);
        });
    } // query
} // rest
