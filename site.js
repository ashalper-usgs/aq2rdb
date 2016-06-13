/**
 * @fileOverview USGS site prototype.
 *
 * @module site
 *
 * @author Andrew Halper <ashalper@usgs.gov>
 */

'use strict';

// Node.js modules
var path = require('path');

// aq2rdb modules
var rest = require('./rest');

var site = module.exports = {
    /**
       @function
       @description Query USGS Site Web Service.
       @public
       @callback
       @param {string} waterServicesHostname Host name of USGS Site
              Web Service server to query.
       @param {string} agencyCode Site's USGS agency code.
       @param {string} siteNumber Site number string (a.k.a. "site ID").
       @param {boolean} log Enable server console logging if true; No
              logging otherwise.
       @param {function} callback Callback function to call when
              complete.
    */
    request: function (
        waterServicesHostname, agencyCode, siteNumber, log, callback
    ) {
        try {
            rest.query(
                waterServicesHostname,
                "GET",
                undefined,      // HTTP headers
                "/nwis/site/",
                {format: "rdb",
                 site: agencyCode + ':' + siteNumber,
                 siteOutput: "expanded"}, log, callback
            );
        }
        catch (error) {
            if (log)
                console.log("site.request: error: " + error);
            callback(error);
        }
        return;
    }, // request

    /**
       @function
       @public
       @description Receive and parse response from USGS Site Web Service.
       @callback
       @param {string} messageBody Message body of HTTP response from USGS
              Site Web Service.
       @param {function} callback Callback to call when complete.
    */
    receive: function (messageBody, callback) {
        var site = new Object;

        /**
           @todo Here we're parsing RDB, which is messy, and would be
                 nice to encapsulate.
        */
        try {
            // parse (station_nm,tz_cd,local_time_fg) from RDB
            // response
            var row = messageBody.split('\n');
            // RDB column names
            var columnName = row[row.length - 4].split('\t');
            // site column values are in last row of table
            var siteField = row[row.length - 2].split('\t');

            // the necessary site fields
            site.agencyCode = siteField[columnName.indexOf('agency_cd')];
            site.number = siteField[columnName.indexOf('site_no')];
            site.name = siteField[columnName.indexOf('station_nm')];
            site.tzCode = siteField[columnName.indexOf('tz_cd')];
            site.localTimeFlag = siteField[columnName.indexOf('local_time_fg')];
        }
        catch (error) {
            callback(error);
            return;
        }

        callback(null, site);
    } // receive

} // site
