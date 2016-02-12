/**
 * @fileOverview Functions to assist with producing RFC 3339 date
 *               strings (used extensively in legacy NWIS, while not
 *               being identified as such).
 *
 * @author <a href="mailto:ashalper@usgs.gov">Andrew Halper</a>
 *
 * @see <a href="https://www.ietf.org/rfc/rfc3339.txt">Date and Time on the Internet: Timestamps</a>.
 */

/**
   @description Public functions.
*/
var rfc3339 = module.exports = {
    /**
       @function Convert an ISO 8601 extended format, date string to
                 RFC 3339 basic format.
       @public
       @param {string} s ISO 8601 date string to convert.
       @see https://tools.ietf.org/html/rfc3339
    */
    toBasicFormat: function (s) {
        return s.replace('T', ' ').replace(/\.\d*/, '');
    }
}
