# File - rdb_header.py
#
# Purpose - Python emulation of legacy NWIS, NW_RDB_HEADER() Fortran
#           subroutine: "Write the rdb header lines to an rdb file".
#
# Authors - Andrew Halper <ashalper@usgs.gov> [translation of NW_RDB_HEADER()]
#           Jeff Christman <jdchrist@usgs.gov> [NW_RDB_HEADER()]
#

from datetime import datetime

def rdb_header(funit):
    funit.write(
        '# //UNITED STATES GEOLOGICAL SURVEY       ' +
        'http://water.usgs.gov/\n'
    )

    funit.write(
        '# //NATIONAL WATER INFORMATION SYSTEM     ' +
        'http://water.usgs.gov/data.html\n'
    )

    funit.write(
    '# //DATA ARE PROVISIONAL AND SUBJECT TO CHANGE UNTIL ' +
        'PUBLISHED BY USGS\n'
    )

    funit.write(
        '# //RETRIEVED: ' +
        datetime.now().strftime('%Y-%m-%d %H:%M:%S\n')
    )
