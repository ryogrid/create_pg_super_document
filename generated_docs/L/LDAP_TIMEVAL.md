# LDAP_TIMEVAL

## Location
src/interfaces/libpq/fe-connect.c: 68 - 74

## Overview
A typedef that aliases the standard POSIX  to  for use in LDAP operations within PostgreSQL's libpq connection library.

## Definition


## Detailed Description
LDAP_TIMEVAL is a simple type alias that maps the standard POSIX  to a name that clearly indicates its use in LDAP contexts. This typedef is defined in the libpq connection module () and is specifically used for setting timeout values when performing LDAP service lookups. The typedef exists to provide semantic clarity when dealing with time-related parameters in LDAP operations, making the code more readable and self-documenting.

The underlying  contains two fields:
- : seconds
- : microseconds

## Parameters / Member Variables
Since this is a typedef of , it inherits the following members:
- : Time value in seconds (time_t type)
- : Time value in microseconds (suseconds_t type)

## Dependencies
- Functions called/Symbols referenced:
  - struct timeval (POSIX standard type)
- Called from (representative examples):
  - ldapServiceLookup (used for LDAP timeout configuration)

## Notes and Other Information
- This typedef is only defined when LDAP support is enabled ( is defined)
- The type is used specifically in  function where it's initialized with  seconds and 0 microseconds
- The typedef provides a layer of abstraction that makes LDAP-related timeout handling more explicit in the code
- Located in 