/**
 * @fileOverview Unit test for aq2rdb.
 *
 * @author <a href="mailto:ashalper@usgs.gov">Andrew Halper</a>
 *
 * @see <a href="https://sites.google.com/a/usgs.gov/nwis_integrator/data_retrieval/cli/aqts2rdb">aqts2rdb</a>.
 */

'use strict';

var assert = require('assert');
var tmp = require('temporary');
var fs = require('fs');
var diff = require('diff');
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

        it('should return "true"', function () {
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

            // check some properties of the diff to work around
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
});
