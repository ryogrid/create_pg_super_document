# check_tuple_field_number

## Location
src/interfaces/libpq/fe-exec.c: 3525 - 3547

## Overview
A static helper function in libpq that validates both tuple (row) and field (column) numbers to ensure they are within valid ranges for a query result set.

## Definition
```c
static int check_tuple_field_number(const PGresult *res, int tup_num, int field_num)
```

## Detailed Description
This function performs comprehensive range validation for both row and column indices in PostgreSQL query results. It first validates that the tuple number falls within the range of 0 to ntups-1, then validates that the field number falls within the range of 0 to numAttributes-1. If either parameter is out of range, it generates an appropriate internal notice message and returns false. This function provides a more thorough validation than check_field_number by also checking row bounds, making it suitable for operations that access specific cells in the result matrix.

## Parameters / Member Variables
- `res`: Pointer to a PGresult structure containing the query result data
- `tup_num`: The tuple (row) number to validate, expected to be 0-based
- `field_num`: The field (column) number to validate, expected to be 0-based

## Dependencies
- Functions called/Symbols referenced:
  - [pqInternalNotice](../p/pqInternalNotice.md) (for error reporting, called twice)
- Called from (representative examples):
  - [PQgetvalue](../P/PQgetvalue.md)
  - [PQgetlength](../P/PQgetlength.md)
  - [PQgetisnull](../P/PQgetisnull.md)

## Notes and Other Information
- Returns true if both tuple and field numbers are valid, false otherwise
- If res is NULL, returns false immediately without error message
- Uses 0-based indexing for both row numbers (0 to ntups-1) and column numbers (0 to numAttributes-1)
- Provides separate error messages for row and column range violations
- Part of libpq's internal validation system for safe cell access in result matrices
- Static function, not exposed in the public libpq API