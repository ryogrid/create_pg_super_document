# check_field_number

## Location
[src/interfaces/libpq/fe-exec.c:3510-3524](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L3510-L3524)

## Overview
A static helper function in libpq that validates whether a given field number is within the valid range for a query result set.

## Definition


## Detailed Description
This function performs range validation for field (column) numbers in PostgreSQL query results. It ensures that the provided field number is valid for the given PGresult structure by checking if it falls within the bounds of 0 to numAttributes-1. If the field number is out of range, it generates an internal notice message and returns false. This function serves as a defensive programming measure to prevent buffer overflows and invalid memory access when accessing result set columns.

## Parameters / Member Variables
- `res`: Pointer to a PGresult structure containing the query result data
- `field_num`: The field (column) number to validate, expected to be 0-based

## Dependencies
- Functions called/Symbols referenced:
  - [pqInternalNotice](../p/pqInternalNotice.md) (for error reporting)
- Called from (representative examples):
  - [PQsetvalue](../P/PQsetvalue.md)
  - [PQfname](../P/PQfname.md)
  - PQftable
  - PQftablecol
  - PQfformat
  - PQftype
  - PQfsize
  - [PQfmod](../P/PQfmod.md)

## Notes and Other Information
- Returns true if the field number is valid, false otherwise
- If res is NULL, returns false immediately without error message
- Uses 0-based indexing for field numbers (0 to numAttributes-1)
- Part of libpq's internal validation system for safe field access
- Static function, not exposed in the public libpq API