/**
 * @fileOverview Encapsulate some legacy NWIS ADAPS utility logic.
 *
 * @author <a href="mailto:ashalper@usgs.gov">Andrew Halper</a>
 */

// Node.js modules
var sprintf = require("sprintf-js").sprintf;

// aq2rdb modules
var rdb = require('./rdb');

var adaps = module.exports = {

    /**
       @class
       @classdesc ADAPS interval of day point type, object
                  prototype. Encapsulates fuzzy math of date intervals
                  found in NWIS ADAPS. Not to be confused with
                  conventional temporal database intervals.
       @public
    */
    IntervalDay: function (from, to, referenceToWaterYear) {
        this.from = rdb.fillBegDate(referenceToWaterYear, from);
        /**
           @todo Check legacy NWIS code to see if there was any
                 initial processing on end date.
        */
        this.to = to;
        this.referenceToWaterYear =
            (referenceToWaterYear === undefined) ? false :
            referenceToWaterYear;
    }, // IntervalDay

    /**
       @description ADAPS interval of second point type, object
                    prototype. Encapsulates fuzzy math of date/time
                    intervals found in NWIS ADAPS. Not to be confused
                    with conventional temporal database intervals.
       @class
       @public
    */
    IntervalSecond: function (from, to, referenceToWaterYear) {
        this.from = rdb.fillBegDtm(referenceToWaterYear, from);
        this.to = rdb.fillEndDtm(referenceToWaterYear, to);
        this.referenceToWaterYear =
            (referenceToWaterYear === undefined) ? false :
            referenceToWaterYear;
    } // IntervalSecond

} // adaps
