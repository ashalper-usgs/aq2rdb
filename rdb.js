/**
 * @fileOverview aq2rdb, USGS-variant RDB utility functions.
 *
 * @author <a href="mailto:ashalper@usgs.gov">Andrew Halper</a>
 * @author <a href="mailto:sbarthol@usgs.gov">Scott Bartholoma</a>
 *
 * @see <a href="https://sites.google.com/a/usgs.gov/nwis_integrator/data_retrieval/cli/aqts2rdb">aqts2rdb</a>.
 */

var moment = require('moment');
var sprintf = require("sprintf-js").sprintf;

var rdb = module.exports = {
    /**
       @function Create RDB header block.
       @public
       @param {string} fileType Type of time series data (e.g. "NWIS-I
              DAILY-VALUES").
       @param {object} site USGS site object.
       @param {string} subLocationIdentifer Sublocation identifier.
       @param {object} parameter USGS parameter (a.k.a. "PARM") object.
       @param {object} type Code and name of type of values
                       [e.g. ("C","COMPUTED")].
       @param {object} range Time series query, date range.
       @param response {object} HTTP response object to write to.
    */
    header: function (
        fileType, site, subLocationIdentifer, parameter, type, range,
        callback
    ) {
        var header =
            "# //UNITED STATES GEOLOGICAL SURVEY " +
                "      http://water.usgs.gov/\n" +
                "# //NATIONAL WATER INFORMATION SYSTEM " +
                "    http://water.usgs.gov/data.html\n" +
                "# //DATA ARE PROVISIONAL AND SUBJECT TO " +
                "CHANGE UNTIL PUBLISHED BY USGS\n" +
        "# //RETRIEVED: " + moment().format("YYYY-MM-DD HH:mm:ss") + '\n';

        /**
           @author <a href="mailto:bdgarner@usgs.gov">Bradley Garner</a>
    
           @todo
           
           Andy,
           I know I've mentioned before we consider going to a release
           without all of these, and then let aggressive testing find the
           gaps.  I still think that's a fine idea that aligns with the
           spirit of minimally viable product.
           
           How do we do this?  Here's an example.
           
           Consider RNDARY="2222233332".  There is nothing like this
           easily available from AQUARIUS API. Yet, AQ API does have the
           new rounding specification, the next & improved evolution in
           how we think of rounding; it looks like SIG(3) as one example.
           Facing this, I can see 3 approaches, in increasing order of
           complexity:
             1) Just stop.  Stop serving RNDARY="foo", assuming most
                people "just wanted the data"
             2) New field. Replace RNDARY with a new element like
                RNDSPEC="foo", which simply relays the new AQUARIUS
                RoundingSpec.
             3) Backward compatibility. Write code that converts a AQ
                rounding spec to a 10-digit NWIS rounding array.  Painful,
                full of assumptions & edge cases.  But surely doable.
          
           In the agile and minimum-vial-product [sic] spirits, I'd
           propose leaning toward (1) as a starting point.  As user
           testing and interaction informs us to the contrary, consider
           (2) or (3) for some fields.  But recognize that option (2) is
           always the most expensive one, so we should do it judiciously
           and only when it's been demonstrated there's a user story
           driving it.
          
           The above logic should work for every field in this header
           block.
          
           Having said all that, some fields are trivially easy to find in
           the AQUARIUS API--that is, option (3) is especially easy, so
           maybe just do them.  In increasing order of difficulty (and
           therefore increasing degrees of warranted-ness):
          
            - LOCATION NAME="foo"  ... This would be the
              SubLocationIdentifer in a GetTimeSeriesDescriptionList()
              call.
            - PARAMETER LNAME="foo" ... is just Parameter as returned by
              GetTimeSeriesDescriptionList()
            - STATISTIC LNAME="foo" ... is ComputationIdentifier +
              ComputationPeriodIdentifier in
              GetTimeSeriesDescriptionList(), although the names will
              shift somewhat from what they would have been in ADAPS which
              might complicate things.
            - DD LABEL="foo" ... Except for the confusing carryover of the
              DD semantic, this should just be some combination of
              Identifier + Label + Comment + Description from
              GetTimeSeriesDescriptionList().  How to combine them, I'm
              not sure, but it should be determinable
            - DD DDID="foo" ...  When and if the extended attribute
              ADAPS_DD is populated in GetTimeSeriesDescriptionList(),
              this is easily populated.  But I think we should wean people
              off this.
            - Note: Although AGING fields might seem simple at first blush
              (the Approvals[] struct from GetTimeSeriesCorrectedData())
              the logic for emulating this old ADAPS format likely would
              get messy in a hurry.
        */

        header +=
            '# //FILE TYPE="' + fileType + '" ' + 'EDITABLE=NO\n' +
            sprintf(
        '# //STATION AGENCY="%-5s" NUMBER="%-15s" TIME_ZONE="%s" DST_FLAG=%s\n',
                site.agencyCode, site.number, site.tzCode,
                site.localTimeFlag
            ) + '# //STATION NAME="' + site.name + '"\n';
    
        /**
           @author <a href="mailto:sbarthol@usgs.gov">Scott Bartholoma</a>
    
           @since 2015-11-11T16:31-07:00
    
           @todo
    
           I think that "# //LOCATION NUMBER=0 NAME="Default"" would
           change to:
           
           # //SUBLOCATION NAME="sublocation name"
           
           and would be omitted if it were the default sublocation and
           had no name.
        */
        if (subLocationIdentifer !== undefined) {
            header += '# //SUBLOCATION ID="' + subLocationIdentifer + '"\n';
        }
    
        /**
           @author <a href="mailto:sbarthol@usgs.gov">Scott Bartholoma</a>
    
           @since 2015-11-11T16:31-07:00
    
           I would be against continuing the DDID field since only
           migrated timeseries will have ADAPS_DD populated.  Instead
           we should probably replace the "# //DD" lines with "#
           //TIMESERIES" lines, maybe something like:
           
           # //TIMESERIES IDENTIFIER="Discharge, ft^3/s@12345678"
           
           and maybe some other information.
        */

        header += "# //PARAMETER CODE=\"" + parameter.code +
            "\" SNAME=\"" + parameter.name + "\"\n" +
            "# //PARAMETER LNAME=\"" + parameter.description + "\"\n";

        if (type)
            header += "# //TYPE CODE=" + type.code + " NAME=" +
                      type.name + "\n";
           
        header += '# //RANGE START="';
        if (range.start !== undefined) {
            header += range.start;
        }
        header += '"';
    
        header += ' END="';
        if (range.end !== undefined) {
            header += range.end;
        }
        /**
           @todo ZONE value should probably be a passed-in parameter
                 at some point.
        */
        header += "\" ZONE=\"LOC\"\n";

        callback(null, header);
    }, // header

    /**
       @function takes an input date with < 8 chars and fills it to 8 chars
       @param wyflag {Boolean} flag if Water Year
       @param begdat {string} input date (may be < 8 chars)
    */
    fillBegDate: function (wyflag, begdat) {
        var iyr, begdate;

        // convert date/times to 8 characters

        if (wyflag) {
            if (begdat.length > 4)
                begdate = begdat.substring(0, 4);
            else
                begdate = begdat;

            iyr = parseInt(begdate);
            if (iyr <= 0)
                begdate = '00000000';
            else
                begdate = sprintf("%4d1001", iyr - 1);
        }
        else {
            if (begdat.length > 8)
                begdate = begdat.substring(0, 8);
            else
                begdate = begdat;
        }

        begdate = sprintf("%8s", begdate).replace(/ /g, '0');

        return begdate;
    }, // fillBegDate

    /**
       @function Node.js emulation of legacy NWIS,
                 NW_RDB_FILL_BEG_DTM() Fortran subroutine: "takes an
                 input date/time and fills it out to 14 chars".

       @param wyflag {Boolean} flag if Water Year
       @param begdat {string} input date/time (may be < 14 chars)
    */
    fillBegDtm: function(wyflag, begdat) {
        var begdtm, iyr;

        // convert date/times to 14 characters

        if (wyflag) {           // output = "<year-1>1001"
            if (begdat.length > 4)      // trim to just the year
                begdtm = begdat.substring(0, 4);
            else
                begdtm = begdat;

            iyr = parseInt(begdtm); // convert to numeric form
            if (iyr <= 0)
                begdtm = "00000000000000";
            else
                // write year-1, month=Oct, day=01
                begdtm = sprintf("%4d1001000000", iyr - 1);

            // Handle beginning of period - needs to be all zeros
            if (begdtm.substring(0, 4) === "0000")
                begdtm = "00000000000000";
        }
        else {                  // regular year, not WY
            if (begdat.length > 14)
                begdtm = begdat.substring(0, 14);
            else
                begdtm = begdat;
        }

        begdtm = sprintf("%14s", begdtm).replace(' ', '0');

        return begdtm;      
    }, // fillBegDtm

    /**
       @function Node.js emulation of legacy NWIS,
                 NW_RDB_FILL_END_DTM() Fortran subroutine: "takes an
                 input date/time and fills it out to 14 chars".
       @author <a href="mailto:ashalper@usgs.gov">Andrew Halper</a>
       @author <a href="mailto:sbarthol@usgs.gov">Scott Bartholoma</a>
       @param wyflag {Boolean} flag if Water Year
       @param enddat {string} input date/time (may be < 14 chars)
    */
    fillEndDtm: function (wyflag, enddat) {
        var enddtm;

        // convert date/times to 14 characters

        if (wyflag) {           // output will be "<year>0930235959"

            if (enddat.length > 4)
                enddtm = enddat.substring(0, 4);
            else
                enddtm = enddat;

            enddtm = sprintf("%4s0930235959", enddtm);
            if (enddtm.substring(0, 4) === "9999")
                enddtm = "99999999999999"; // end of period is all nines
            
        }
        else {                  // output will be filled-out date/time

            if (enddat.length > 14)
                enddtm = enddat.substring(0, 14);
            else
                enddtm = enddat;

        }

        enddtm = sprintf("%14s", enddtm);
        if (enddtm.substring(8, 14) === ' ') {
            if (enddtm.substring(0, 8) === "99999999")
                enddtm = enddtm.substr(0, 8) +  "999999";
            else
                enddtm = enddtm.substr(0, 8) + "235959";
        }
        enddtm = enddtm.replace(' ', '0');
        return enddtm;
    } // fillEndDtm

} // rdb
