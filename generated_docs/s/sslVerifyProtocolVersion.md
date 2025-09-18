# sslVerifyProtocolVersion

## Location
src/interfaces/libpq/fe-connect.c: 7588 - 7613

## Overview
Validates SSL protocol version strings used in PostgreSQL connection parameters to ensure they specify supported TLS versions.

## Definition


## Detailed Description
This function serves as a sanity check routine for the connection parameters  and . It validates that the input string represents a supported SSL/TLS protocol version. The function accepts standard TLS version identifiers and treats empty strings or NULL values as valid (equivalent to ignoring the parameter).

## Parameters / Member Variables
- : A string containing the SSL/TLS protocol version to validate (e.g., "TLSv1.2")

## Dependencies
- Functions called/Symbols referenced:
  - strlen (standard C library function)
  - pg_strcasecmp (PostgreSQL case-insensitive string comparison)
- Called from (representative examples):
  - internalPQconninfoOption
  - pqConnectOptions2
  - sslVerifyProtocolRange

## Notes and Other Information
- Accepts TLS versions: "TLSv1", "TLSv1.1", "TLSv1.2", "TLSv1.3"
- Empty strings and NULL values are considered valid
- Uses case-insensitive comparison for version strings
- Returns true for valid versions, false otherwise
- Static function scope limited to fe-connect.c