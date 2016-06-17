/**
 * @fileOverview USGS data domain prototypes.
 *
 * @module usgs
 *
 * @author Andrew Halper <ashalper@usgs.gov>
 */

'use strict';

class Site {
    constructor(locationIdentiferString) {
        var locationIdentifier =
            new aquaticInformatics.LocationIdentifier(
                locationIdentiferString
            );

        this.agencyCode = locationIdentifier.agencyCode();
        this.number = locationIdentifier.siteNumber();
    } // constructor

    init() {
        /**
           @see http://www.tomas-dvorak.cz/posts/nodejs-request-without-dependencies/
        */
        var getSiteRDB = new Promise((resolve, reject) => {
            const request = http.get(
                "http://" + options.waterServicesHostname +
                    "/nwis/site/?" + querystring.stringify(
                        {format: "rdb",
                         site: this.agencyCode + ":" + this.number,
                         siteOutput: "expanded"}
                    ),
                (response) => {
                    // handle HTTP errors
                    if (response.statusCode < 200 ||
                        299 < response.statusCode) {
                        reject(response.statusCode);
                        return;
                    }
                    // temporary data holder
                    const body = [];
                    // on every content chunk, push it to the data array
                    response.on("data", (chunk) => body.push(chunk));
                    // we are done, resolve promise with those joined chunks
                    response.on("end", () => resolve(body.join("")));
                });
            // handle connection errors of the request
            /**
               @todo errors need to be triaged and the ultimate error
                     messages delivered to the client made more
                     helpful here.
            */
            request.on("error", (error) => reject(error));
        });

        // expose run-time scope of "this" to scope of promise below
        var instance = this;

        return getSiteRDB.then((messageBody) => {
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
                instance.number = siteField[columnName.indexOf("site_no")];
                instance.name = siteField[columnName.indexOf("station_nm")];
                instance.tzCode = siteField[columnName.indexOf("tz_cd")];
                instance.localTimeFlag =
                    siteField[columnName.indexOf("local_time_fg")];
            }
            catch (error) {
                throw error;
            }
        }).catch((error) => {
            throw error;
        });
    } // init
} // Site
