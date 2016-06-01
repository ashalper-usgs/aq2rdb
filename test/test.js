/**
 * @fileOverview Unit tests for aq2rdb.
 *
 * @author <a href="mailto:ashalper@usgs.gov">Andrew Halper</a>
 *
 * @see <a href="https://sites.google.com/a/usgs.gov/nwis_integrator/data_retrieval/cli/aqts2rdb">aqts2rdb</a>.
 */

'use strict';

/**
   @description You're running the test, so you must want the Node.js
                environment to be "test", at least as long as the test
                is running.
   @see http://stackoverflow.com/questions/11104028/process-env-node-env-is-undefined
*/
process.env.NODE_ENV = "test";

// Node.JS modules
var assert = require("assert");
var diff = require("diff");
var expect = require("chai").expect;
var fs = require("fs");
var http = require("http");
var sinon = require("sinon");

// aq2rdb modules
var aq2rdb = require("../aq2rdb.js");
var adaps = require("../adaps.js");
var aquaticInformatics = require("../aquaticInformatics.js");
var rdb = require("../rdb.js");
var rest = require("../rest.js");
var service = require("../service.js");

describe("adaps", function () {
    describe("#IntervalDay", function () {
        describe("#()", function () {
            it("should construct", function () {
                var from = "19690218", to = "20160218";
                var interval = new adaps.IntervalDay(from, to, false);

                assert.equal(from, interval.from);
                assert.equal(to, interval.to);
            });
        }); // ()
    }); // IntervalDay

    describe("#IntervalSecond", function () {
        describe("#()", function () {
            it("should construct", function () {
                var from = "19690218000000", to = "20160218000000";
                var interval = new adaps.IntervalSecond(from, to, false);

                assert.equal(from, interval.from);
                assert.equal(to, interval.to);
            });
        });
    });
}); // adaps

describe("aq2rdb", function () {
    /**
       @description Default/parse command-line options before running
                    all tests.
    */
    before(function (done) {
        aq2rdb._private.options = aq2rdb._private.cli.parse();
        done();
    });

    describe(
        "#handle()", function () {
            var mockResponse;

            beforeEach(function () {
                mockResponse = sinon.mock({
                    writeHead: function (statusCode, headers) {},
                    write: function (body) {},
                    end: function () {}
                });

                mockResponse.expects("writeHead").once();
                mockResponse.expects("end").once();
            });

            it("should handle ECONNREFUSED correctly", function () {
                aq2rdb._private.handle(
                    {code: "ECONNREFUSED"}, mockResponse.object
                );
            });
            it("should handle ReferenceErrors correctly", function () {
                aq2rdb._private.handle(
                    (new ReferenceError), mockResponse.object
                );
            });
            it("should handle errors typed as \"string\" correctly",
               function () {
                   aq2rdb._private.handle(
                       "It seems there was an error", mockResponse.object
                   );
               });
            it("should handle all other errors correctly", function () {
                aq2rdb._private.handle(
                    {message: "It seems there was an error"},
                    mockResponse.object
                );
            });
        }
    );
    describe("#toNWISDateFormat()", function () {
        it("should return \"19690218\"", function () {
            assert.equal(
                "19690218",
                aq2rdb.toNWISDateFormat("1969-02-18T07:30:00.000")
            );
        });
    });
    describe("#toNWISTimeFormat()", function () {
        it("should return \"073000\"", function () {
            assert.equal(
                "073000",
                aq2rdb.toNWISTimeFormat("1969-02-18T07:30:00.000")
            );
        });
    });
    describe("/aq2rdb", function() {
    });
    /** @see https://mochajs.org/#asynchronous-code */
    describe("AQUARIUS", function () {
        var aquarius;

        this.timeout(10000);    // AQUARIUS can be slow

        describe("#()", function () {
            it("should throw 'Required field \"hostname\" not found' error",
               function (done) {
                   aquarius = new aquaticInformatics.AQUARIUS(
                       "localhost",
                       undefined,
                       aq2rdb._private.options.aquariusUserName,
                       "Not a password",
                       function (error) {
                           expect(error).equals(
                               "Required field \"hostname\" not found"
                           );
                           done();
                       }
                   );
               });

            it("should throw 'Required field \"hostname\" must have " +
               "a value' error",
               function (done) {
                   aquarius = new aquaticInformatics.AQUARIUS(
                       "localhost",
                       "",
                       aq2rdb._private.options.aquariusUserName,
                       "Not a password",
                       function (error) {
                           expect(error).equals(
                               "Required field \"hostname\" must have " +
                                   "a value"
                           );
                           done();
                       }
                   );
               });

            it("should throw 'Required field \"userName\" not found' error",
               function (done) {
                   aquarius = new aquaticInformatics.AQUARIUS(
                       "localhost",
                       aq2rdb._private.options.aquariusHostname,
                       undefined,
                       "Not a password",
                       function (error) {
                           expect(error).equals(
                               "Required field \"userName\" not found"
                           );
                           done();
                       }
                   );
               });

            it("should throw 'Required field \"userName\" must have " +
               "a value' error",
               function (done) {
                   aquarius = new aquaticInformatics.AQUARIUS(
                       "localhost",
                       aq2rdb._private.options.aquariusHostname,
                       "",
                       "Not a password",
                       function (error) {
                           expect(error).equals(
                               "Required field \"userName\" must have " +
                                   "a value"
                           );
                           done();
                       }
                   );
               });

            it("should have non-empty-string token",
               function (done) {
                   aquarius = new aquaticInformatics.AQUARIUS(
                       "localhost",
                       aq2rdb._private.options.aquariusHostname,
                       /**
                          @see http://stackoverflow.com/questions/16144455/mocha-tests-with-extra-options-or-parameters
                       */
                       process.env.AQUARIUS_USER_NAME,
                       process.env.AQUARIUS_PASSWORD,
                       function (error) {
                           if (error) throw error;
                           expect(aquarius.token().length).to.be.above(0);
                           done();
                       }
                   );
               });

        }); // #()

        describe("#getLocationData()", function () {
            it("should receive a usable LocationDataServiceResponse object",
               function (done) {
                   aquarius.getLocationData(
                       "09380000", // COLORADO RIVER AT LEES FERRY, AZ
                       function (error, messageBody) {
                           if (error) throw error;
                           var locationDataServiceResponse =
                               JSON.parse(messageBody);
                           expect(
                               Object.getOwnPropertyNames(
                                   locationDataServiceResponse
                               ).length).to.be.above(0);
                           done();
                       });
               });
        }); // #getLocationData()
        describe("#getTimeSeriesDescription()", function () {
            var siteNo = "09380000"; // COLORADO RIVER AT LEES FERRY, AZ

            it("should receive a usable TimeSeriesDescription " +
               "object for LocationIdentifier " + siteNo,
               function (done) {
                   aquarius.getTimeSeriesDescription(
                       "USGS", siteNo, "Discharge", "Instantaneous",
                       "Points",
                       function (error, timeSeriesDescription) {
                           if (error) throw error;
                           expect(
                               Object.getOwnPropertyNames(
                                   timeSeriesDescription
                               ).length).to.be.above(0);
                           done();
                       }
                   );
               });

            siteNo = "01646500";
            it("should receive a \"More than one primary time " +
               "series found...\" error message",
               function (done) {
                   aquarius.getTimeSeriesDescription(
                       "USGS", siteNo, "Specific cond at 25C",
                       undefined, "Daily",
                       function (error, timeSeriesDescription) {
                           assert.equal(
                               error,
    "More than one primary time series found for \"01646500\":\n" +
    "#\n" +
    "#   Specific cond at 25C.uS/cm.From multiparameter sonde.Max@01646500\n" +
    "#   Specific cond at 25C.uS/cm.From multiparameter sonde.Mean@01646500\n" +
    "#   Specific cond at 25C.uS/cm.From multiparameter sonde.Min@01646500\n" +
    "#   Specific cond at 25C.uS/cm.Max@01646500\n" +
    "#   Specific cond at 25C.uS/cm.Mean@01646500\n" +
    "#   Specific cond at 25C.uS/cm.Min@01646500\n"
                           );
                           done();
                       }
                   );
               });
        });
        describe("#getTimeSeriesCorrectedData()", function () {
            it("should receive a usable TimeSeriesDataServiceResponse object",
               function (done) {
                   aquarius.getTimeSeriesCorrectedData(
                       {TimeSeriesUniqueId: "7050c0c28bb8409295ef0e82ceda936e",
                        ApplyRounding: "true",
                        QueryFrom: "2014-10-01T00:00:00-07:00:00",
                        QueryTo: "2014-10-02T00:00:00-07:00:00"},
                       function (error, timeSeriesDataServiceResponse) {
                           if (error) throw error;
                           var timeSeriesDescriptions =
                               JSON.parse(timeSeriesDataServiceResponse);
                           expect(
                               Object.getOwnPropertyNames(
                                   timeSeriesDescriptions
                               ).length).to.be.above(0);
                           done();
                       }
                   );
               });          
        }); // #getTimeSeriesCorrectedData()

    }); // AQUARIUS

}); // aq2rdb

describe("aquaticInformatics", function () {
    var locationIdentifier;

    describe("#LocationIdentifier()", function () {
        it("should construct", function () {
            locationIdentifier =
                new aquaticInformatics.LocationIdentifier(
                    "USGS", "123456789012345"
                );
        });
    });

    describe("#LocationIdentifier().agencyCode()", function () {
        it("should be \"USGS\"", function () {
            assert.equal(locationIdentifier.agencyCode(), "USGS");
        });
    });

    describe("#LocationIdentifier().siteNumber()", function () {
        it("should be \"123456789012345\"", function () {
            assert.equal(locationIdentifier.siteNumber(),
                         "123456789012345");
        });
    });

    describe("#LocationIdentifier().toString()", function () { 
        it("should be \"123456789012345\"", function () {
            assert.equal(locationIdentifier.toString(),
                         "123456789012345");
        });

        it("should be \"123456789012345-USFS\"", function () {
            // reconstruct location identifier to check different path
            // in toString() method
            var locationIdentifier = new aquaticInformatics.LocationIdentifier(
                "USFS", "123456789012345"
            );

            assert.equal(locationIdentifier.toString(),
                         "123456789012345-USFS");
        });
    });
});

describe("rdb", function () {
    describe("#header()", function () {
        it("should match", function (done) {
            rdb.header(
                "NWIS-I DAILY-VALUES", // fileType
                "YES",                 // editable
                // site
                {agencyCode: "USGS",
                 number: "123456789012345",
                 name: "NOT A SITE",
                 tzCode: "MST",
                 localTimeFlag: 'Y'},
                undefined,      // subLocationIdentifer
                // parameter
                {code: "00060", name: "Discharge",
                 description: "Discharge, cubic feet per second"},
                // statistic
                {code: "00003", name: "MEAN", description: "MEAN VALUES"},
                // type
                {name: "FINAL",
                 description: "EDITED AND COMPUTED DAILY VALUES"},
                {start: "20141001", end: "20150930"}, // range
                function (error, header) {
                    // Messy, but there is evidently a problem with
                    // passing pattern strings to
                    // expect().to.match(). See
                    // https://github.com/jmendiara/karma-jquery-chai/issues/3
                    // for more.
                    expect(header).to.match(
/^# \/\/UNITED STATES GEOLOGICAL SURVEY       http:\/\/water.usgs.gov\/\n# \/\/NATIONAL WATER INFORMATION SYSTEM     http:\/\/water.usgs.gov\/data.html\n# \/\/DATA ARE PROVISIONAL AND SUBJECT TO CHANGE UNTIL PUBLISHED BY USGS\n# \/\/RETRIEVED: \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\n# \/\/FILE TYPE="NWIS-I DAILY-VALUES" EDITABLE=NO\n# \/\/STATION AGENCY="USGS " NUMBER="123456789012345" TIME_ZONE="MST" DST_FLAG=Y\n# \/\/STATION NAME="NOT A SITE"\n# \/\/PARAMETER CODE="00060" SNAME="Discharge"\n# \/\/PARAMETER LNAME="Discharge, cubic feet per second"\n# \/\/STATISTIC CODE="00003" SNAME="MEAN"\n# \/\/STATISTIC LNAME="MEAN VALUES"\n# \/\/TYPE NAME="FINAL" DESC = "EDITED AND COMPUTED DAILY VALUES"\n# \/\/RANGE START="20141001" END="20150930"/);
                    done();
                }
            );
        });
    }); // #header()

    describe("#fillBegDtm()", function () {
        // wyflag === true

        it("should return \"20121001000000\"", function () {
            assert.equal(rdb.fillBegDtm(true, "201310010000"),
                         "20121001000000");
        });

        it("should return \"20121001000000\"", function () {
            assert.equal(rdb.fillBegDtm(true, "2013"),
                         "20121001000000");
        });

        it("should return \"00000000000000\"", function () {
            assert.equal(rdb.fillBegDtm(true, "-2013"),
                         "00000000000000");
        });

        it("should return \"00000000000000\"", function () {
            assert.equal(rdb.fillBegDtm(true, "0000"),
                         "00000000000000");
        });

        // wyflag === false

        it("should return \"20131001000000\"", function () {
            // truncate 15 character date to 14 characters
            assert.equal(rdb.fillBegDtm(false, "201310010000000"),
                         "20131001000000");
        });

        it("should return \"20131001000000\"", function () {
            // complete start date-time seconds place
            assert.equal(rdb.fillBegDtm(false, "201310010000"),
                         "20131001000000");
        });

        it("should return \"20131001000000\"", function () {
            // suffix white space
            assert.equal(rdb.fillBegDtm(false, "20131001    "),
                         "20131001000000");
        });

    });

    describe("#fillEndDtm()", function () {
        // wyflag === true

        // year-typed point value to second-typed point value aligned
        // on end of water year interval
        it("should return \"20130930235959\"", function () {
            assert.equal(rdb.fillEndDtm(true, "2013"), "20130930235959");
        });

        // align dubious date to water year interval end date-time
        // point
        it("should return \"20130930235959\"", function () {
            assert.equal(rdb.fillEndDtm(true, "20130"), "20130930235959");
        });

        // end-user-exposed, representation of "until the end of time"
        // predicate
        it("should return \"99999999999999\"", function () {
            assert.equal(rdb.fillEndDtm(true, "9999"), "99999999999999");
        });

        // wyflag === false

        // default end date-time seconds place
        it("should return \"20131001000000\"", function () {
            assert.equal(rdb.fillEndDtm(false, "201310010000"),
                         "20131001000000");
        });

        // truncate 15 character date to 14 characters
        it("should return \"20131001000000\"", function () {
            assert.equal(rdb.fillEndDtm(false, "201310010000000"),
                         "20131001000000");
        });

        it("should return \"99999999999999\"", function () {
            assert.equal(rdb.fillEndDtm(false, "99999999      "),
                         "99999999999999");
        });

        it("should return \"19990909235959\"", function () {
            assert.equal(rdb.fillEndDtm(false, "19990909      "),
                         "19990909235959");
        });

    });

});
