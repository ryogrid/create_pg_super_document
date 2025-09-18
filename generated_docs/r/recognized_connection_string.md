# recognized_connection_string

## Location
src/interfaces/libpq/fe-connect.c: 5842 - 5852

## Overview
Determines whether a given string is a recognized PostgreSQL connection string format by checking for URI prefix or key-value pair syntax.

## Definition


## Detailed Description
This function validates whether a connection string follows one of PostgreSQL's accepted formats. It performs a preliminary check to determine if the string appears to be a valid connection string that can be parsed by PostgreSQL's connection parsing routines. The function checks for two main patterns:

1. **URI format**: Strings that start with a valid URI prefix (like "postgresql://" or "postgres://")
2. **Key-value format**: Strings that contain an equals sign ("="), indicating parameter=value pairs

This is a lightweight validation function that must be consistent with  - any string that returns true from this function should be parseable by the actual parsing routines. The function is noted as a duplicate of the eponymous libpq function, indicating it exists in multiple places in the codebase for different components.

## Parameters / Member Variables
- : A null-terminated string containing the potential connection string to validate

## Dependencies
- Functions called/Symbols referenced:
  - [uri_prefix_length](../u/uri_prefix_length.md)
  - strchr (standard C library function)
- Called from (representative examples):
  - [do_connect](../d/do_connect.md) (src/bin/psql/command.c:3402)
  - HeadMatchesCS (src/bin/psql/tab-complete.c:4814, 4819)
  - internalPQconninfoOption (src/interfaces/libpq/fe-connect.c:412)
  - PQsetdbLogin (src/interfaces/libpq/fe-connect.c:1938)
  - [conninfo_array_parse](../c/conninfo_array_parse.md) (src/interfaces/libpq/fe-connect.c:6055)

## Notes and Other Information
- This function is implemented in multiple locations (noted as a duplicate of libpq function)
- Located in psql's common.c, indicating it's used by the psql client specifically
- Serves as a fast pre-validation before attempting actual connection string parsing
- The validation is intentionally permissive - it may accept some strings that will ultimately fail during actual parsing
- Return value: true if the string appears to be a connection string, false otherwise