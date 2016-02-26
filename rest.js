var http = module.exports = {
    /**
       @function Call a REST Web service with an HTTP query; send response
       via a callback.
       @private
       @param {string} host Host part of HTTP query URL.
       @param {string} path Path part of HTTP query URL.
       @param {object} field An array of attribute-value pairs to bind in
       HTTP query URL.
       @param {function} callback Callback function to call if/when
       response from Web service is received.
    */
    query: function (host, path, obj, callback) {
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
                callback(null, messageBody);
                return;
            });
        }

        path += '?' + querystring.stringify(obj);

        var request = http.request({
            host: host,
            path: path
        }, queryCallback);

        /**
           @description Handle service invocation errors.
        */
        request.on('error', function (error) {
            callback(error);
            return;
        });

        request.end();
    } // query
} // http
