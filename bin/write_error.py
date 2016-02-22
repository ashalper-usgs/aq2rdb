# File - write_error.py
#
# Purpose - Python emulation of legacy NWIS, NW_WRITE_ERROR() Fortran
#           subroutine: "Write the contents of the error static memory
#           area as an error to the file open on the unit number
#           supplied."
#
# Authors - Andrew Halper <ashalper@usgs.gov> [Python translation]
#           Scott D. Bartholoma <sbarthol@usgs.gov> [NW_WRITE_ERROR()]
#
def write_error(return_cd, module_nm, operation_cd, operation_tx,
                error_tx, ounit):
      if return_cd != 0:
          # convert the error number to a character string
          cerr = "{}".format(return_cd)

          # write the message to the supplied unit
          ounit.write(
              '\n' +
              'Error ' + cerr + ' on operation "' + operation_cd + '"\n' +
              ' in module "' + module_nm + '"\n' +
              ' while ' + operation_tx + ':\n' +
              ' ' + error_tx
          )
