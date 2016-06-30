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
var expect = require("chai").expect;
var fs = require("fs");
var http = require("http");
require("should");
require("should-http");
var sinon = require("sinon");
var request = require("supertest");  

// aq2rdb modules
var aq2rdb = require("../aq2rdb.js");
var adaps = require("../adaps.js");
var aquaticInformatics = require("../aquaticInformatics.js");
var rdb = require("../rdb.js");
var rest = require("../rest.js");

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
    describe("#handle()", function () {
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
    describe("#appendIntervalSearchCondition()", function () {
        it("should be equal", function () {
            assert.deepEqual(
                {QueryFrom: "2013-10-01T00:00:00-07:00",
                 QueryTo: "2013-10-14T00:00:00-07:00"},
                aq2rdb._private.appendIntervalSearchCondition(
                    {}, {from: "20131001", to: "20131014"}, "MST",
                    "00000000", "99999999", function (error) {
                        throw new Error(error);
                    }
                )
            );
        });
    });
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

    describe("AQUARIUS", function () {
        var aquarius;

        describe("#()", function () {
            it("should throw 'Required field \"hostname\" not found' error",
               function () {
                   try {
                       aquarius = new aquaticInformatics.AQUARIUS(
                           "localhost",
                           undefined,
                           aq2rdb._private.options.aquariusUserName,
                           "Not a password"
                       );
                   }
                   catch (error) {
                       expect(error).equals(
                           "Required field \"hostname\" not found"
                       );
                   }
               });

            it("should throw 'Required field \"hostname\" must have " +
               "a value' error",
               function () {
                   try {
                       aquarius = new aquaticInformatics.AQUARIUS(
                           "localhost",
                           "",
                           aq2rdb._private.options.aquariusUserName,
                           "Not a password"
                       );
                   }
                   catch (error) {
                       expect(error).equals(
                           "Required field \"hostname\" must have " +
                               "a value"
                       );
                   }
               });

            it("should throw 'Required field \"userName\" not found' error",
               function () {
                   try {
                       aquarius = new aquaticInformatics.AQUARIUS(
                           "localhost",
                           aq2rdb._private.options.aquariusHostname,
                           undefined,
                           "Not a password"
                       );
                   }
                   catch (error) {
                       expect(error).equals(
                           "Required field \"userName\" not found"
                       );
                   }
               });

            it("should throw 'Required field \"userName\" must have " +
               "a value' error",
               function () {
                   try {
                       aquarius = new aquaticInformatics.AQUARIUS(
                           "localhost",
                           aq2rdb._private.options.aquariusHostname,
                           "",
                           "Not a password"
                       );
                   }
                   catch (error) {
                       expect(error).equals(
                           "Required field \"userName\" must have " +
                               "a value"
                       );
                   }
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
                       process.env.AQUARIUS_PASSWORD
                   );
                   aquarius.authenticate().then(() => {
                       expect(aquarius.token().length).to.be.above(0);
                       done();
                   });
               });

        }); // #()

        describe("#getLocationData()", function () {
            it("should receive a usable LocationDataServiceResponse object",
               function (done) {
                   aquarius.getLocationData(
                       "09380000" // COLORADO RIVER AT LEES FERRY, AZ
                   ).then((messageBody) => {
                       var locationDataServiceResponse =
                           JSON.parse(messageBody);

                       expect(
                           Object.getOwnPropertyNames(
                               locationDataServiceResponse
                           ).length).to.be.above(0);
                       done();
                   }).catch((error) => {throw error;});
               });
        }); // #getLocationData()

        describe("#getTimeSeriesDescription()", function () {
            var siteNo = "09380000"; // COLORADO RIVER AT LEES FERRY, AZ

            it("should receive a usable TimeSeriesDescription " +
               "object for LocationIdentifier " + siteNo,
               function (done) {
                   aquarius.getTimeSeriesDescription(
                       "USGS", siteNo,
                       "Discharge", "Instantaneous", "Points"
                   ).then((timeSeriesDescription) => {
                       expect(Object.getOwnPropertyNames(
                           timeSeriesDescription
                       ).length).to.be.above(0);
                       done();
                   }).catch((error) => {throw error;});
               });

            it("should receive a \"More than one primary time " +
               "series found...\" error message",
               function (done) {
                   aquarius.getTimeSeriesDescription(
                       "USGS", "01646500", "Specific cond at 25C",
                       undefined, "Daily"
                   ).catch((error) => {
                       assert(
                           error.startsWith(
                'More than one primary time series found for "01646500":\n' +
                                   '#\n' +
                                   '#   '
                           )
                       );
                       done();
                   });
               });

            /** @see JIRA issue AQRDB-33 */
            it("should throw \"No time series description list " +
               "found...\" error",
               function (done) {
                   aquarius.getTimeSeriesDescription(
                       "USGS", "XXXXXXXX",
                       "Discharge", "Instantaneous", "Points"
                   )
                       .catch((error) => {
                           assert.equal(
                               error.startsWith(
                                   "No time series description list found at "
                               ),
                               true
                           );
                           done();
                       });
               });
        });

        /**
           @todo need to (re-)write a more sophisticated test for
                 aquaticInformatics.AQUARIUS.getTimeSeriesCorrectedData()
                 here, because as might be expected,
                 TimeSeriesUniqueId values do not persist across
                 different AQUARIUS server instances.
        */

        this.timeout(4000);     // needs some extra time
        describe("#getRemarkCodes()", function () {
            it("should load remark codes",
               function (done) {
                   aquarius.getRemarkCodes()
                       .then(() => {
                           expect(
                               Object.keys(aquarius.remarkCodes).length
                           ).to.be.above(0);
                           done();
                       })
                       .catch((error) => {throw error;});
               });
        }); // #getRemarkCodes()
        
    }); // AQUARIUS
}); // aquaticInformatics

describe("rdb", function () {
    describe("#header()", function () {
        it("should match", function () {
            var header = rdb.header(
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
                {start: "20141001", end: "20150930"} // range
            );

            // Messy, but there is evidently a problem with passing
            // pattern strings to expect().to.match(). See
            // https://github.com/jmendiara/karma-jquery-chai/issues/3
            // for more.
            expect(header).to.match(
/^# \/\/UNITED STATES GEOLOGICAL SURVEY       http:\/\/water.usgs.gov\/\n# \/\/NATIONAL WATER INFORMATION SYSTEM     http:\/\/water.usgs.gov\/data.html\n# \/\/DATA ARE PROVISIONAL AND SUBJECT TO CHANGE UNTIL PUBLISHED BY USGS\n# \/\/RETRIEVED: \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\n# \/\/FILE TYPE="NWIS-I DAILY-VALUES" EDITABLE=NO\n# \/\/STATION AGENCY="USGS " NUMBER="123456789012345" TIME_ZONE="MST" DST_FLAG=Y\n# \/\/STATION NAME="NOT A SITE"\n# \/\/PARAMETER CODE="00060" SNAME="Discharge"\n# \/\/PARAMETER LNAME="Discharge, cubic feet per second"\n# \/\/STATISTIC CODE="00003" SNAME="MEAN"\n# \/\/STATISTIC LNAME="MEAN VALUES"\n# \/\/TYPE NAME="FINAL" DESC = "EDITED AND COMPUTED DAILY VALUES"\n# \/\/RANGE START="20141001" END="20150930"/);
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
}); // rdb

 /*
   These routing tests require aq2rdb to running on localhost. We
   don't start aq2rdb from test.js because it requires
   user-name/password authentication credentials for prerequisite Web
   services (e.g., AQUARIUS) that we can't have in plaintext. You'll
   need to do that from the command-line if these tests are to
   succeed. See ../README.md for more.
 */
describe("routing", function () {
    var url = "http://localhost:8081";

    /**
       @see https://thewayofcode.wordpress.com/2013/04/21/how-to-build-and-test-rest-api-with-nodejs-express-mocha/
    */
    describe("aq2rdb", function () {
        it("should GET DV RDB for USGS 09380000", function (done) {
            // aq2rdb -n09380000 -tdv -p00065 -s00003 -b20131001 -e20131014
            var fields = {
                n: "09380000",
                t: "dv",
                p: "00065",
                s: "00003",
                b: "20131001",
                e: "20131014"
            };
            request(url)
                .get("/aq2rdb")
                .send(fields)
                .end(        // handles the response
                    function (error, response) {
                        if (error)
                            throw error;
                        response.should.have.status(200);
                        done();
                    }
                );
        });

        it("should GET UV RDB for USGS 09380000", function (done) {
            // aq2rdb -n09380000 -tuv -sC -p00065 -b201310010000 -e201310140000
            var fields = {
                n: "09380000",
                t: "uv",
                p: "00065",
                s: "C",
                b: "201310010000",
                e: "201310140000"
            };
            request(url)
                .get("/aq2rdb")
                .send(fields)
                .end(        // handles the response
                    function (error, response) {
                        if (error)
                            throw error;
                        response.should.have.status(200);
                        done();
                    }
                );
        });

        /**
           @see https://usgs.slack.com/archives/aq2rdb/p1465229527000003
        */
        it("should GET UV RDB for USGS 09180500", function (done) {
            // aq2rdb -tuv -sc -n09180500 -p00060 -b20101001000000
            //        -e20101002000000
            var fields = {
                n: "09180500",
                t: "uv",
                p: "00060",
                s: "c",
                b: "20101001000000",
                e: "20101002000000"
            };
            request(url)
                .get("/aq2rdb")
                .send(fields)
                .end(        // handles the response
                    function (error, response) {
                        if (error)
                            throw error;
                        response.should.have.status(200);
                        done();
                    }
                );
        });

        /**
           @see https://usgs.slack.com/archives/aq2rdb/p1465229527000003
        */
        it("should GET correct time points UV RDB for USGS 09380000",
           function (done) {
               // aq2rdb -n09380000 -sC -tuv -p00065 -b201310010000
               // -e201310150000
               var fields = {
                   n: "09380000",
                   t: "uv",
                   p: "00065",
                   s: "C",
                   b: "201310010000",
                   e: "201310150000"
               };
               request(url)
                   .get("/aq2rdb")
                   .send(fields)
                   .end(        // handles the response
                       function (error, response) {
                           /**
                              @todo check dates at begin/end of interval
                                    for correctness
                           */
                           if (error)
                               throw error;
                           response.should.have.status(200);
                           done();
                       }
                   );
           });

        describe("GetDVTable", function () {
            it("should GET DV RDB from aq2rdb/GetDVTable for USGS 09380000",
               function (done) {
                   var fields = {
                       LocationIdentifier: "09380000",
                       Parameter: "Discharge",
                       QueryFrom: "2015-01-01T00:00:00-07:00",
                       QueryTo: "2015-01-03T00:00:00-07:00"
                   };

                   request(url)
                       .get("/aq2rdb/GetDVTable")
                       .send(fields)
                       .end(function (error, response) {
                           if (error)
                               throw error;
                           response.should.have.status(200);
                           done();
                       });
               });
        });
    });
});
