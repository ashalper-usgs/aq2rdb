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
var tmp = require('temporary');
var fs = require('fs');
var diff = require('diff');
var expect = require('chai').expect;
var aq2rdb = require('../aq2rdb.js');

describe('Array', function() {
    describe('#rdbHeader()', function () {
        var reference, rdbHeaderFile, rdbHeader;
        
        /**
           @description Make a temporary file containing an aq2rdb RDB
                        header block, for comparison purposes.
           @see http://mochajs.org/#hooks
        */
        before(function() {
            reference = fs.readFileSync('USGS-01010000.rdb', 'ascii');
            rdbHeaderFile = new tmp.File();

            rdbHeaderFile.open('w');
            rdbHeaderFile.writeFileSync(
                aq2rdb.rdbHeader(
                    'NWIS-I UNIT-VALUES',
                    {agencyCode: 'USGS', number: '01010000',
                     name: 'St. John River at Ninemile Bridge, Maine',
                     tzCode: 'EST', localTimeFlag: 'Y'},
                    undefined,  // subLocationIdentifer
                    {start: '19951231', end: '19961231'}
                )
            );
            rdbHeader = fs.readFileSync(rdbHeaderFile.path, 'ascii');
        });

        it('should be true', function () {
            var results = diff.diffLines(reference, rdbHeader);
            var retrieved =
                /^# \/\/RETRIEVED: \d{4}-\d\d-\d\d \d\d:\d\d:\d\d UTC$/;
            var added = false, removed = false;

            results.forEach(function (part) {
                if (part.added === true &&
                    part.value.match(retrieved) !== null) {
                    added = true;
                }
                if (part.removed === true &&
                    part.value.match(retrieved) !== null) {
                    removed = true;
                }
            });

            // check some properties of the RDB diff to work around
            // datetimes in RETRIEVED field being (correctly)
            // different
            assert.equal(4, results.length);
            assert.equal(false, added);
            assert.equal(false, removed);
        });

        /**
           @description Clean up temporary file created in before()
                        call.
           @see http://mochajs.org/#hooks
        */
        after(function() {
            rdbHeaderFile.unlinkSync();
        });
    });
    describe('#toBasicFormat()', function () {
        it('should return "1969-02-18 07:30:00"', function () {
            assert.equal(
                '1969-02-18 07:30:00',
                aq2rdb.toBasicFormat('1969-02-18T07:30:00.000')
            );
        });
    });
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
    describe('#receiveSite()', function () {
        var messageBody =
            fs.readFileSync('USGS-Site-Web-Service.rdb', 'ascii');
        var referenceSite = {
            agencyCode: 'USGS', number: '01010000',
            name: 'St. John River at Ninemile Bridge, Maine',
            tzCode: 'EST', localTimeFlag: 'Y'
        };
        var testSite;

        // send RDB file as string to receiveSite(), using callback to
        // set results to testSite, so Chai can compare it with
        // reference site below
        aq2rdb._private.receiveSite(
            messageBody,
            function (error, site) {testSite = site;}
        );

        it('should be true', function () {
            expect(referenceSite).to.deep.equal(testSite);
        });
    });
});
