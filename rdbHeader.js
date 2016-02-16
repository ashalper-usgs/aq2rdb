/**
 * @fileOverview Node.js emulation of legacy NWIS, NW_RDB_HEADER(),
 *               Fortran subroutine: "Write the rdb header lines to an
 *               rdb file".
 *
 * @author <a href="mailto:ashalper@usgs.gov">Andrew Halper</a>
 * @author <a href="mailto:jcorn@usgs.gov">James Cornwall</a>
 * @author <a href="mailto:jdchrist@usgs.gov">Jeff Christman</a>
 *
 * @see <a href="https://sites.google.com/a/usgs.gov/nwis_integrator/data_retrieval/cli/aqts2rdb">aqts2rdb</a>.
 */

var moment = require('moment');

function rdbHeader(response) {
    // cruft from Fortran
    var suffix = '                    \n';

    response.write(
        '# //UNITED STATES GEOLOGICAL SURVEY ' +
            '      http://water.usgs.gov/' + suffix +
            '# //NATIONAL WATER INFORMATION SYSTEM ' +
            '    http://water.usgs.gov/data.html' + suffix +
            '# //DATA ARE PROVISIONAL AND SUBJECT TO ' +
            'CHANGE UNTIL PUBLISHED BY USGS' + suffix +
            '# //RETRIEVED: ' + moment().format('YYYY-MM-DD HH:mm:ss')
    );
}
