# PQgetvalue

## Location
src/interfaces/libpq/fe-exec.c: 3876 - 3886

## Overview
PQgetvalue retrieves the value of a specific field (column) from a specific row in a PostgreSQL query result set.

## Definition


## Detailed Description
PQgetvalue is a fundamental function in libpq for extracting data from query results. It returns a pointer to the string representation of the value at the specified row and column position. The function performs bounds checking to ensure the requested tuple (row) and field (column) numbers are valid before accessing the data. If the validation fails, it returns NULL and generates an internal notice message.

The returned value is a null-terminated string representation of the field data. For binary format results, this would be the binary data, but most commonly it returns text representations. The returned pointer points to storage within the PGresult structure and should not be freed by the caller.

## Parameters / Member Variables
- : Pointer to the PGresult structure containing the query results
- : Zero-based row number (tuple index) to retrieve data from
- : Zero-based column number (field index) to retrieve data from

## Dependencies
- Functions called/Symbols referenced:
  - [check_tuple_field_number](../c/check_tuple_field_number.md)
- Called from (representative examples):
  - (No direct references found in the codebase - typically called by client applications)

## Notes and Other Information
- Returns NULL if the tuple or field number is out of range
- The returned string should not be modified or freed by the caller
- For NULL database values, this function returns an empty string - use PQgetisnull() to distinguish between empty strings and NULL values
- The returned pointer is valid only as long as the PGresult structure exists
- This is one of the most commonly used functions in libpq client applications for data retrieval