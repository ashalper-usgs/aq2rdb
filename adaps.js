/**
 * @fileOverview Encapsulate some legacy NWIS ADAPS utility logic.
 *
 * @module adaps
 *
 * @author Andrew Halper <ashalper@usgs.gov>
 */

// aq2rdb modules
var rdb = require('./rdb');

var adaps = module.exports = {

    /**
       @class
       @classdesc ADAPS interval of day point type, object
                  prototype. Encapsulates fuzzy math of date intervals
                  found in NWIS ADAPS. Not to be confused with
                  conventional, temporal database interval concept.
       @public
       @param {string} from Start date of interval.
       @param {string} to End date of interval.
       @param {boolean} referenceToWaterYear Reference interval to
              water year if true; reference to calendar year
              otherwise.
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
                    with conventional, temporal database interval
                    concept.
       @class
       @public
       @param {string} from Start date/time of interval.
       @param {string} to End date/time of interval.
       @param {boolean} referenceToWaterYear Reference interval to
              water year if true; reference to calendar year
              otherwise.
    */
    IntervalSecond: function (from, to, referenceToWaterYear) {
        this.from = rdb.fillBegDtm(referenceToWaterYear, from);
        this.to = rdb.fillEndDtm(referenceToWaterYear, to);
        this.referenceToWaterYear =
            (referenceToWaterYear === undefined) ? false :
            referenceToWaterYear;
    } // IntervalSecond

} // adaps
