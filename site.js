/**
 * @fileOverview Functions for querying USGS Site Web Service.
 *
 * @author <a href="mailto:ashalper@usgs.gov">Andrew Halper</a>
 */

'use strict';

/**
   @description Public functions.
*/
var site = module.exports = {
    /**
       @function Query USGS Site Web Service.
       @callback
       @param {string} siteNumber NWIS site number string.
    */
    request: function (waterServicesHostname, number, log, callback) {
        if (log)
            console.log(
                path.basename(process.argv[1]).slice(0, -3) +
                    '.request.number: ' + number
            );

        try {
            httpQuery(
                waterServicesHostname, '/nwis/site/',
                {format: 'rdb',
                 sites: number,
                 siteOutput: 'expanded'}, callback
            );
        }
        catch (error) {
            callback(error);
        }
        return;
    }, // request

    /**
       @function Receive and parse response from USGS Site Web Service.
       @callback
       @param {string} messageBody Message body of HTTP response from USGS
       Site Web Service.
       @param {function} callback Callback to call when complete.
    */
    receive: function(messageBody, callback) {
        var site = new Object;

        /**
           @todo Here we're parsing RDB, which is messy, and would be nice
           to encapsulate.
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
