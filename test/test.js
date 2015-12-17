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
            reference = fs.readFileSync('USGS-01010000.rdb');
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

            rdbHeader = rdbHeaderFile.readFileSync();
        });
        describe('#rdbHeader()', function () {
            /**
               @todo Re-write to ignore datetime in "//RETRIEVED"
               field.
            */
            it('should return equal', function () {
                assert.equal(reference, rdbHeader);
            });
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
