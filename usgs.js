/**
 * @fileOverview USGS data domain prototypes.
 *
 * @module usgs
 *
 * @author Andrew Halper <ashalper@usgs.gov>
 */

'use strict';

var aquaticInformatics = require("./aquaticInformatics");
var rest = require("./rest");

var usgs = module.exports = {

Site: function (locationIdentiferString) {
    var locationIdentifier =
        new aquaticInformatics.LocationIdentifier(
            locationIdentiferString
        );

    this.agencyCode = locationIdentifier.agencyCode();
    this.number = locationIdentifier.siteNumber();

    this.load = function (host) {
        var instance = this;

        return rest.query(
            "http", host, "GET", undefined, "/nwis/site/?",
            {format: "rdb",
             site: instance.agencyCode + ":" + instance.number,
             siteOutput: "expanded"}, false
        )
            .then((messageBody) => {
                try {
                    // parse (station_nm,tz_cd,local_time_fg) from RDB
                    // response
                    var row = messageBody.split('\n');
                    // RDB column names
                    var columnName = row[row.length - 4].split('\t');
                    // site column values are in last row of table
                    var siteField = row[row.length - 2].split('\t');

                    // the necessary site fields
                    instance.agencyCode =
                        siteField[columnName.indexOf("agency_cd")];
                    instance.number =
                        siteField[columnName.indexOf("site_no")];
                    instance.name =
                        siteField[columnName.indexOf("station_nm")];
                    instance.tzCode =
                        siteField[columnName.indexOf("tz_cd")];
                    instance.localTimeFlag =
                        siteField[columnName.indexOf("local_time_fg")];
                }
                catch (error) {
                    throw error;
                }
            }).catch((error) => {
                throw error;
            });
    } // load
} // Site

} // usgs
