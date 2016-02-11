# File - rdb_write_loc_info.py
#
# Purpose - Python emulation of legacy NWIS, NW_RDB_WRITE_LOC_INFO()
#           Fortran subroutine: "Write the location information to an
#           rdb file".
#
# Authors - Andrew Halper <ashalper@usgs.gov> [Python translation]
#           Scott D. Bartholoma <sbarthol@usgs.gov> [NW_RDB_WRITE_LOC_INFO()]
#

def rdb_write_loc_info(funit, dd_id):
    # **********************************************************************
    # * FUNCTION DECLARATIONS
    # **********************************************************************

    # logical nw_db_retr_loc_for_dd,
    # *     nw_db_retr_reflist_cd_nm,
    # *     nw_va_rrnd_tx
    #  integer nwf_strlen

    # **********************************************************************
    # * EXTERNAL SUBROUTINES OR FUNCTIONS
    # **********************************************************************

    # **********************************************************************
    # * INCLUDE FILES
    # **********************************************************************

    #  include 'adaps_keys.ins'

    # **********************************************************************
    # * LOCAL VARIABLE DECLARATIONS
    # **********************************************************************
    #
    #  integer loc_nu
    #  character loc_nm*30,
    # *     loc_ds*300,
    # *     loc_x_sec_cd*4,
    # *     loc_x_sec_sg*1,
    # *     loc_x_sec_rd*1,
    # *     loc_elev_cd*4,
    # *     loc_elev_sg*1,
    # *     loc_elev_rd*1,
    # *     code_nm*64,
    # *     code_tx*20,
    # *     cloc_nu*12
    #  real*4 loc_x_sec_va,
    # *     loc_elev_va
    #
    # **********************************************************************
    # * FORMATS
    # **********************************************************************

    # get the location information; see also
    # http://nwists.usgs.gov/AQUARIUS/Publish/v2/json/metadata?op=LocationDescriptionListServiceRequest
    loc_nu, loc_nm, loc_ds, loc_x_sec_cd, loc_x_sec_va, loc_x_sec_sg, \
        loc_x_sec_rd, loc_elev_cd, loc_elev_va, loc_elev_sg, \
        loc_elev_rd = nw_db_retr_loc_for_dd(dd_id) # <- TODO

    if not loc_nu:
        # location not found, or other error; set number and name to
        # dummy bad values
        loc_nu = -1
        loc_nm = '*** LOC NOT FOUND ***'

    # write number and name
    cloc_nu = "{:<12}".format(loc_nu)

    if loc_nu == 0: loc_nm = 'Default'

    # write (funit,2060) '# //LOCATION NUMBER=',
    # *     cloc_nu(1:nwf_strlen(cloc_nu)),
    # *     ' NAME="',loc_nm(1:nwf_strlen(loc_nm)),'"'
    # 2060 FORMAT (20A)
    funit.write('# //LOCATION NUMBER=' + cloc_nu + ' NAME="' + '"\n')

    # if location was found, and it was not the default location,
    # write other information
    if loc_nu > 0:
        # write description, if any
        if loc_ds != ' ':
            # write (funit,2060) '# //LOCATION DESCRIPTION="',
            # *           loc_ds(1:nwf_strlen(loc_ds)),'"'
            funit.write('# //LOCATION DESCRIPTION="' + loc_ds + '"')

        # write xsec info if any
        code_nm = ' '
        code_tx = ' '
        if loc_x_sec_cd != 'UNSP':
            if not nw_db_retr_reflist_cd_nm('loc_x_sec', loc_x_sec_cd, code_nm):
                code_nm = ' '

        if loc_x_sec_va > NW_CR4:
            if not nw_va_rrnd_tx(loc_x_sec_va, loc_x_sec_rd, code_tx):
                code_tx = ' '

        if code_nm != ' ' or code_tx != ' ':
            # write (funit,2060) '# //LOCATION XSECNAME="',
            # *           code_nm(1:nwf_strlen(code_nm)),'" XSECVALUE="',
            # *           code_tx(1:nwf_strlen(code_tx)),'"'
            funit.write(
                '# //LOCATION XSECNAME="' + code_nm +
                '" XSECVALUE="' + code_tx + '"'
            )

        # write elev info if any
        code_nm = ' '
        code_tx = ' '
        if loc_elev_cd != 'UNSP':
            if not nw_db_retr_reflist_cd_nm ('loc_elev', loc_elev_cd, code_nm):
                code_nm = ' '

        if loc_elev_va > NW_CR4:
            if not nw_va_rrnd_tx(loc_elev_va, loc_elev_rd, code_tx):
                code_tx = ' '

        if code_nm != ' ' or code_tx != ' ':
            # write (funit,2060) '# //LOCATION ELEVNAME="',
            # *           code_nm(1:nwf_strlen(code_nm)),'" ELEVVALUE="',
            # *           code_tx(1:nwf_strlen(code_tx)),'"'
            funit.write(
                '# //LOCATION ELEVNAME="' + code_nm +
                '" ELEVVALUE="' + code_tx + '"'
            )

    return
