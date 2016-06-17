/**
 * @fileOverview aq2rdb, USGS-variant RDB utility functions.
 *
 * @module rdb
 *
 * @author Andrew Halper <ashalper@usgs.gov>
 * @author Scott Bartholoma <sbarthol@usgs.gov>
 *
 * @see <a href="https://sites.google.com/a/usgs.gov/nwis_integrator/data_retrieval/cli/aqts2rdb">aqts2rdb</a>.
 */

var moment = require('moment');
var sprintf = require("sprintf-js").sprintf;

var rdb = module.exports = {
    /**
       @function
       @description Create RDB header block.
       @public
       @param {string} fileType Type of time series data (e.g. "NWIS-I
              DAILY-VALUES").
       @param {string} editable "YES" if file is intended to be
                       editable; "NO" otherwise.
       @param {object} site USGS site object.
       @param {string} subLocationIdentifer Sublocation identifier.
       @param {object} parameter USGS parameter (a.k.a. "PARM") object.
       @param {object} statistic USGS statistic object.
       @param {object} type Code and name of type of values
                       [e.g. ("C","COMPUTED")].
       @param {object} range Time series query, date range.
    */
    header: function (
        fileType, editable, site, subLocationIdentifer, parameter,
        statistic, type, range
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
           @author Bradley Garner <bdgarner@usgs.gov>

           @todo
           
           <div>Andy,</div>

           <p>
           I know I've mentioned before we consider going to a release
           without all of these, and then let aggressive testing find the
           gaps.  I still think that's a fine idea that aligns with the
           spirit of minimally viable product.
           </p>
           
           <p>
           How do we do this?  Here's an example.
           </p>
           
           <p>Consider <code>RNDARY="2222233332"</code>. There is
           nothing like this easily available from AQUARIUS API. Yet,
           AQ API does have the new rounding specification, the next &
           improved evolution in how we think of rounding; it looks
           like SIG(3) as one example.  Facing this, I can see 3
           approaches, in increasing order of complexity:</p>

           <ol>
             <li>Just stop. Stop serving <code>RNDARY="foo"</code>,
             assuming most people "just wanted the data"</li>
             <li>New field. Replace <code>RNDARY</code> with a new element
             like <code>RNDSPEC="foo"</code>, which simply relays the
             new AQUARIUS <code>RoundingSpec</code>.</li>
             <li>Backward compatibility. Write code that converts a AQ
             rounding spec to a 10-digit NWIS rounding array. Painful,
             full of assumptions & edge cases. But surely doable.</li>
           </ol>

           <p>In the agile and minimum-vial-product [sic] spirits, I'd
           propose leaning toward (1) as a starting point.  As user
           testing and interaction informs us to the contrary, consider
           (2) or (3) for some fields.  But recognize that option (2) is
           always the most expensive one, so we should do it judiciously
           and only when it's been demonstrated there's a user story
           driving it.</p>
          
           <p>The above logic should work for every field in this header
           block.</p>
          
           <p>Having said all that, some fields are trivially easy to find in
           the AQUARIUS API&mdash;that is, option (3) is especially easy, so
           maybe just do them.  In increasing order of difficulty (and
           therefore increasing degrees of warranted-ness):</p>
           <ul>
            <li><code>LOCATION NAME="foo"</code>&hellip;This would be the
              <code>SubLocationIdentifer</code> in a
              <code>GetTimeSeriesDescriptionList()</code> call.</li>
            <li><code>PARAMETER LNAME="foo"</code>&hellip;is just
              Parameter as returned by
              GetTimeSeriesDescriptionList()</li> <li><code>STATISTIC
              LNAME="foo"</code>&hellip;is <code>ComputationIdentifier
              + ComputationPeriodIdentifier</code> in
              <code>GetTimeSeriesDescriptionList()</code>, although
              the names will shift somewhat from what they would have
              been in ADAPS which might complicate things.</li>
            <li><code>DD LABEL="foo"</code>&hellip;Except for the
              confusing carryover of the DD semantic, this should just
              be some combination of <code>Identifier + Label +
              Comment + Description</code> from
              <code>GetTimeSeriesDescriptionList()</code>. How to
              combine them, I'm not sure, but it should be
              determinable</li>
            <li><code>DD DDID="foo"</code>&hellip;When and if the
              extended attribute <code>ADAPS_DD</code> is populated in
              <code>GetTimeSeriesDescriptionList()</code>, this is easily
              populated. But I think we should wean people off
              this.</li>
            <li>Note: Although <code>AGING</code> fields might seem
              simple at first blush (the <code>Approvals[]</code>
              struct from <code>GetTimeSeriesCorrectedData()</code>)
              the logic for emulating this old ADAPS format likely
              would get messy in a hurry.</li>
            </ul>
        */
        header +=
            '# //FILE TYPE="' + fileType + '" ' + 'EDITABLE=NO\n' +
            sprintf(
       '# //STATION AGENCY="%-5s" NUMBER="%-15s" TIME_ZONE="%s" DST_FLAG=%s\n',
                site.agencyCode, site.number, site.tzCode,
                site.localTimeFlag
            ) + '# //STATION NAME="' + site.name + '"\n';
    
        /**
           @author Scott Bartholoma <sbarthol@usgs.gov>
           @since 2015-11-11T16:31-07:00
        
           @description <p>I think that "<code># //LOCATION NUMBER=0
                        NAME="Default"</code>" would change to:</p>
           
           <p># //SUBLOCATION NAME="sublocation name"</p>
           
           <p>and would be omitted if it were the default sublocation and
           had no name.</p>
        */

        /**
           @author Wade Walker <walker@usgs.gov>
           @since 2016-02-16T08:30-07:00

           @description sublocation is the AQUARIUS equivalent of
                        ADAPS location. It is returned from any of the
                        <code>GetTimeSeriesDescriptionList</code>&hellip;methods
                        or for <code>GetFieldVisitData</code> method
                        elements where sublocation is
                        appropriate. <code>GetSensorsAndGages</code>
                        will also return associated
                        sublocations. They're basically just a shared
                        attribute of time series, sensors and gages,
                        and field readings, so no specific call for
                        them, they're just returned with the data
                        they're applicable to. Let me know if you need
                        something beyond that.
        */
        if (subLocationIdentifer !== undefined) {
            header += '# //SUBLOCATION ID="' + subLocationIdentifer + '"\n';
        }
    
        /**
           @author Scott Bartholoma <sbarthol@usgs.gov>
           @since 2015-11-11T16:31-07:00
    
           @description <p>I would be against continuing the
                        <code>DDID</code> field since only migrated
                        timeseries will have <code>ADAPS_DD</code>
                        populated. Instead we should probably replace
                        the "<code># //DD</code>" lines with "<code>#
                        //TIMESERIES</code>" lines, maybe something
                        like:</p>
           
           <p>
           <code># //TIMESERIES IDENTIFIER="Discharge, ft^3/s@12345678"</code>
           </p>
           
           <p>and maybe some other information.</p>
        */

        header += "# //PARAMETER CODE=\"" + parameter.code +
            "\" SNAME=\"" + parameter.name + "\"\n" +
            "# //PARAMETER LNAME=\"" + parameter.description + "\"\n";

        if (statistic !== undefined) {
            header += "# //STATISTIC CODE=\"" + statistic.code +
                "\" SNAME=\"" + statistic.name + "\"\n" +
                "# //STATISTIC LNAME=\"" + statistic.description +
                "\"\n";
        }

        if (type)
            header += "# //TYPE NAME=\"" + type.name + "\" DESC = \"" +
                      type.description + "\"\n";

        /**
           @todo <p>write data aging information:</p>

           <p><code>rdb_write_aging(funit, dbnum, dd_id, begdate, enddate);</code></p>
        */

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
           @todo nwts2rdb "-l" option?

           header += "\" ZONE=\"LOC\"\n";
        */
        header += "\"\n";

        return header;
    }, // header

    /**
       @function
       @description Takes an input date with < 8 chars and fills it to
                    8 characters.
       @param {boolean} wyflag Flag if Water Year.
       @param {string} begdat Input date (may be < 8 chars).
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
       @function
       @description Node.js emulation of legacy NWIS,
                    <code>NW_RDB_FILL_BEG_DTM()</code> Fortran
                    subroutine: "takes an input date/time and fills it
                    out to 14 chars".
       @param {boolean} wyflag Flag if Water Year.
       @param {string} begdat Input date/time (may be < 14 chars).
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

        begdtm = sprintf("%-14s", begdtm).replace(/\s/g, '0');

        return begdtm;      
    }, // fillBegDtm

    /**
       @function
       @description Node.js emulation of legacy NWIS,
                    <code>NW_RDB_FILL_END_DTM()</code> Fortran
                    subroutine: "takes an input date/time and fills it
                    out to 14 chars".
       @author Andrew Halper <ashalper@usgs.gov>
       @author Scott Bartholoma <sbarthol@usgs.gov>
       @param {boolean} wyflag Flag if Water Year.
       @param {string} enddat Input date/time (may be < 14 chars).
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

        enddtm = sprintf("%-14s", enddtm);
        if (enddtm.substring(8, 14) === "      ") {
            if (enddtm.substring(0, 8) === "99999999")
                enddtm = enddtm.substr(0, 8) +  "999999";
            else
                enddtm = enddtm.substr(0, 8) + "235959";
        }
        enddtm = enddtm.replace(/\s/g, '0');
        return enddtm;
    } // fillEndDtm

} // rdb
