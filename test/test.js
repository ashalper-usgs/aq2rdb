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

describe('Array', function() {
    describe(
        '#handle()', function () {
            var mockResponse;

            beforeEach(function() {
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
    /** @description Test AQUARIUS prototype constructor. */
    describe('#AQUARIUS()', function () {
        var aquarius, msg = "\"aquarius\" object constructed", cbMsg;

        beforeEach(function() {
            aquarius = new aq2rdb._private.AQUARIUS(
                aq2rdb._private.cli.aquariusHostname,
                aq2rdb._private.cli.aquariusUserName,
                aq2rdb._private.cli.aquariusPassword,
                function () { cbMsg = msg; }
            );
        });

        it("should return \"" + msg + "\"", function () {
            /**
               @todo this could be more sophisticated, by checking one
                     of "aquarius" object's property values.
            */
            assert.equal(msg, cbMsg);
        });
    });
});
