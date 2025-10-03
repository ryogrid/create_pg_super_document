# PQlibVersion

## Location
[src/interfaces/libpq/fe-misc.c:63-76](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-misc.c#L63-L76)

## Overview
Returns the version number of the libpq library as an integer value.

## Definition

```c
int
PQlibVersion(void)
```
## Detailed Description
PQlibVersion is a simple utility function that returns the version number of the PostgreSQL libpq library. It returns the compile-time PostgreSQL version number (PG_VERSION_NUM) which represents the version of PostgreSQL that libpq was built against. This function allows client applications to programmatically determine which version of libpq they are using, which can be useful for version-specific feature detection or compatibility checks.

The version number is encoded as an integer where the format typically follows PostgreSQL's version numbering scheme (e.g., version 17.6 would be represented as an appropriate integer encoding).

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - PG_VERSION_NUM (compile-time constant representing PostgreSQL version)
- Called from (representative examples):
  - Client applications requiring libpq version information

## Notes and Other Information
- This is a libpq client library function, not a server-side function
- The function is declared in libpq-fe.h and implemented in fe-misc.c
- Returns a compile-time constant, so the value is determined when libpq is built
- Useful for applications that need to adapt their behavior based on libpq version capabilities
- Part of the public libpq API for client applications