/**
 * @fileOverview Unit test for aq2rdb.
 *
 * @author <a href="mailto:ashalper@usgs.gov">Andrew Halper</a>
 *
 * @see <a href="https://sites.google.com/a/usgs.gov/nwis_integrator/data_retrieval/cli/aqts2rdb">aqts2rdb</a>.
 */

'use strict';

var assert = require('assert');
var aq2rdb = require('../aq2rdb.js');
    
describe('Array', function() {
    describe('#toBasicFormat()', function () {
        it('should return "19690218"', function () {
            assert.equal(
                '1969-02-18 07:30:00',
                aq2rdb.toBasicFormat('1969-02-18T07:30:00.000')
            );
        });
    });
});
