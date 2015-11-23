# aq2rdb
USGS-variant, RDB output from AQUARIUS.

aq2rdb is a Web service, designed to provide some (but not complete) backwards-compatibility features for the NWIS CLI program nwts2rdb. The goal of aq2rdb is to ease the transition from CLI nwts2rdb and its dependents to the AQUARIUS Web service API.

Presently, aq2rdb provides only one Web service "endpoint": the `GetDVTable` service for obtaining site-referenced, derived statistical "daily values" from AQUARIUS for a given measurement parameter. `GetDVTable` accepts a set of HTTP query field/value pairs, and will respond with an RDB file (a TSV file with some additional specific conventions) containing any daily values found.

A similar endpoint called `GetUVTable` for serving measured "unit values" (a.k.a. "raw values"), is under development.
