# aq2rdb
USGS-variant, RDB output from AQUARIUS.

aq2rdb is a Web service, designed to provide some (but not comprehensive) backwards-compatibility features for the NWIS CLI program `nwts2rdb`. The goal of aq2rdb is to ease the transition from CLI `nwts2rdb` and its dependents to the AQUARIUS Web service API.

Presently, aq2rdb provides two Web service "endpoints": `GetDVTable` and `GetUVTable`.

The `GetDVTable` service serves site-referenced, derived statistical "daily values" from AQUARIUS for a given measurement parameter. `GetDVTable` accepts a set of HTTP query field/value pairs, and will respond with an RDB file (a TSV file with some additional specific conventions) containing any daily values found.

The `GetUVTable` service has a similar interface to `GetDVTable`, but serves measured "unit values" (a.k.a. "raw values") instead.

## Installing From Source
Clone the aq2rdb repository on GitHub:

    git clone https://github.com/ashalper-usgs/aq2rdb.git

Run the `npmbuild` script in the newly created source directory:

    sh npmbuild
    
This script is safe to run repeatedly, even if you have some or all required npm packages already installed.

## Installing the npm Package

## Running
aq2rdb is started from the Node.js interpreter like this:

    node aq2rdb.js --aquariusUserName aquariusUserName \
                   --aquariusPassword aquariusPassword \
                   --waterDataUserName waterDataUserName \
                   --waterDataPassword waterDataPassword

where *aquariusUserName* is the user name of an AQUARIUS account under
which aq2rdb will query AQUARIUS Web services, *aquariusPassword* is
the password for this account; *waterDataUserName* is the user name of
an NWIS-RA account under which aq2rdb will query NWIS-RA Web services,
and *waterDataPassword* is the password for this account.

Other aq2rdb command-line arguments have reasonable default values,
but may be overridden by specifying them.

## Testing
There is a suite of Mocha tests in the `test/` subdirectory. Presently, for the tests to run successfully, there will need to be an `aquarius-token` server running the `GetAQToken` service locally, and the environment variables `AQUARIUS_USER_NAME` and `AQUARIUS_PASSWORD` environment variables will need to be set accordingly. These are the counterparts of the aq2rdb server's `--aquariusUserName` and `--aquariusPassword` command-line options; unfortunately there is currently no way to pass these to Mocha via the CLI.

If in a hurry, one can also run these "quick-and-dirty" curl commands vs. a running aq2rdb server:

    curl 'http://localhost:8081/aq2rdb?p=00060&t=uv&s=C&n=09380000&b=20141001000000&e=20141002000000'
    curl 'http://localhost:8081/aq2rdb?p=00060&t=dv&s=00003&n=09380000&b=20141001&e=20150930'
