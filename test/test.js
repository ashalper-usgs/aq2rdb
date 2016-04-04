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
process.env.NODE_ENV = 'test';

var assert = require('assert');
var diff = require('diff');
var expect = require('chai').expect;
var fs = require('fs');
var http = require('http');
var sinon = require('sinon');
var tmp = require('temporary');

var aq2rdb = require('../aq2rdb.js');
var rest = require('../rest.js');

describe('aq2rdb', function () {
    /**
       @description Default/parse command-line options before running
                    all tests.
    */
    before(function (done) {
        aq2rdb._private.options = aq2rdb._private.cli.parse();
        done();
    });

    describe(
        '#handle()', function () {
            var mockResponse;

            beforeEach(function () {
                mockResponse = sinon.mock({
                    writeHead: function (statusCode, headers) {},
                    write: function (body) {},
                    end: function () {}
                });

                mockResponse.expects('writeHead').once();
                mockResponse.expects('end').once();
            });

            it('should handle ECONNREFUSED correctly', function () {
                aq2rdb._private.handle(
                    {code: 'ECONNREFUSED'}, mockResponse.object
                );
            });
            it('should handle ReferenceErrors correctly', function () {
                aq2rdb._private.handle(
                    (new ReferenceError), mockResponse.object
                );
            });
            it('should handle errors typed as "string" correctly', function () {
                aq2rdb._private.handle(
                    'It seems there was an error', mockResponse.object
                );
            });
            it('should handle all other errors correctly', function () {
                aq2rdb._private.handle(
                    {message: 'It seems there was an error'},
                    mockResponse.object
                );
            });
        }
    );
    describe('#toNWISDateFormat()', function () {
        it('should return "19690218"', function () {
            assert.equal(
                '19690218',
                aq2rdb.toNWISDateFormat('1969-02-18T07:30:00.000')
            );
        });
    });
    describe('#toNWISTimeFormat()', function () {
        it('should return "073000"', function () {
            assert.equal(
                '073000',
                aq2rdb.toNWISTimeFormat('1969-02-18T07:30:00.000')
            );
        });
    });
    /** @see https://mochajs.org/#asynchronous-code */
    describe('AQUARIUS', function () {
        var aquarius;

        describe('#()', function () {
            it('should throw \'Required field "hostname" not found\' error',
               function (done) {
                   aquarius = new aq2rdb._private.AQUARIUS(
                       undefined,
                       aq2rdb._private.options.aquariusUserName,
                       "Not a password",
                       function (error) {
                           expect(error).equals(
                               'Required field "hostname" not found'
                           );
                           done();
                       }
                   );
               });

            it('should throw \'Required field "hostname" must have ' +
               'a value\' error',
               function (done) {
                   aquarius = new aq2rdb._private.AQUARIUS(
                       "",
                       aq2rdb._private.options.aquariusUserName,
                       "Not a password",
                       function (error) {
                           expect(error).equals(
                               'Required field "hostname" must have ' +
                                   'a value'
                           );
                           done();
                       }
                   );
               });

            it('should throw \'Required field "userName" not found\' error',
               function (done) {
                   aquarius = new aq2rdb._private.AQUARIUS(
                       aq2rdb._private.options.aquariusHostname,
                       undefined,
                       "Not a password",
                       function (error) {
                           expect(error).equals(
                               'Required field "userName" not found'
                           );
                           done();
                       }
                   );
               });

            it('should throw \'Required field "userName" must have ' +
               'a value\' error',
               function (done) {
                   aquarius = new aq2rdb._private.AQUARIUS(
                       aq2rdb._private.options.aquariusHostname,
                       "",
                       "Not a password",
                       function (error) {
                           expect(error).equals(
                               'Required field "userName" must have ' +
                                   'a value'
                           );
                           done();
                       }
                   );
               });

            it('should have non-empty-string token',
               function (done) {
                   aquarius = new aq2rdb._private.AQUARIUS(
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
        describe('#getLocationData()', function () {
            it('should receive a usable LocationDataServiceResponse object',
               function (done) {
                   aquarius.getLocationData(
                       '09380000', // COLORADO RIVER AT LEES FERRY, AZ
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
        describe('#getTimeSeriesCorrectedData()', function () {
            it('should receive a usable TimeSeriesDataServiceResponse object',
               function (done) {
                   aquarius.getTimeSeriesCorrectedData(
                       {TimeSeriesUniqueId: '7050c0c28bb8409295ef0e82ceda936e',
                        ApplyRounding: 'true',
                        QueryFrom: '2014-10-01T00:00:00-07:00:00',
                        QueryTo: '2014-10-02T00:00:00-07:00:00'},
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
        /** @todo needs work before we can call AQUARIUS.distill() */
        describe('#distill()', function () {
            it('should have a usable TimeSeriesDescriptionList',
               function (done) {
                   rest.query(
                       aquarius.hostname,
                       "GET",
                       undefined,      // HTTP headers
                       "/AQUARIUS/Publish/V2/GetTimeSeriesDescriptionList",
                       {token: aquarius.token(), format: "json",
                        LocationIdentifier: "09380000",
                        Parameter: "Discharge",
                        ComputationPeriodIdentifier: "Daily",
                        ExtendedFilters:
                        "[{FilterName:ACTIVE_FLAG,FilterValue:Y}]"},
                       false,
                       function (error, messageBody) {
                           if (error) throw error;

                           var timeSeriesDescriptionListServiceResponse =
                               JSON.parse(messageBody);

                           var timeSeriesDescriptions =
               timeSeriesDescriptionListServiceResponse.TimeSeriesDescriptions;

                           expect(
                               Object.getOwnPropertyNames(
                                   timeSeriesDescriptions
                               ).length).to.be.above(0);

                           done();
                       }
                   );
               });
        }); // #distill()
    }); // AQUARIUS
});
