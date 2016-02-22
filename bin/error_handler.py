# File - error_handler.py
#
# Purpose - Python emulation of legacy NWIS, nw_error_handler() C
#           function: "Loads an error message into the error static
#           memory area"
#
# Authors - Andrew Halper <ashalper@usgs.gov> [Python translation]
#           Scott D. Bartholoma <sbarthol@usgs.gov> [nw_error_handler()]
#

def error_handler(
        error_nu, # The return code to store in the error static memory area
        module_nm, # The name of the subroutine that had the error
        db_op_cd,  # A short (8 character) operation code
        operation_tx,      # What was being done when the error ocurred
        error_tx           # A verbose description of the error
):
    # This module loads a non-sql error message into the error static
    # memory area for later use and display
    #
    # Routines that use this function should call it with an error_nu
    # of 0 on successful completion (or call nw_clear_error) to clear
    # the error static memory area of any errors from other previous
    # routines that used this function.
    #
    # There was an error:
    #
    # sprintf (error_tx,"DDID \"%s\" is not a valid integer",cddid);
    #
    # nw_error_handler(1234,"dd_transfer","ddconv",
    #                  "converting DDID to an integer",
    #                  error_tx);
    #
    # Successful completion:
    #   
    #   nw_error_handler (0,"dd_transfer","ddconv",
    #                     "converting DDID to an integer",
    #                     "Successful completion");

    # load the error number
    error_block = {
        'code': error_nu, 'subr': module_nm[0:31],
        'type': db_op_cd[0:7], 'op_tx': operation_tx[0:99],
        'text': error_tx[0:863]
    }

    return error_block
