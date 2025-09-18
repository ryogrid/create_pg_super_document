# check_param_number

## Location
src/interfaces/libpq/fe-exec.c: 3548 - 3566

## Overview
A static helper function in libpq that validates whether a given parameter number is within the valid range for a prepared statement's parameter list.

## Definition
```c
static int check_param_number(const PGresult *res, int param_num)
```

## Detailed Description
This function performs range validation for parameter numbers in PostgreSQL prepared statement results. It ensures that the provided parameter number is valid for the given PGresult structure by checking if it falls within the bounds of 0 to numParameters-1. If the parameter number is out of range, it generates an internal notice message and returns false. This function is specifically designed for validating access to parameter metadata in prepared statements, complementing the field and tuple validation functions for comprehensive result set safety.

## Parameters / Member Variables
- `res`: Pointer to a PGresult structure containing the prepared statement result data
- `param_num`: The parameter number to validate, expected to be 0-based

## Dependencies
- Functions called/Symbols referenced:
  - pqInternalNotice (for error reporting)
- Called from (representative examples):
  - PQparamtype

## Notes and Other Information
- Returns true if the parameter number is valid, false otherwise
- If res is NULL, returns false immediately without error message
- Uses 0-based indexing for parameter numbers (0 to numParameters-1)
- Specifically designed for prepared statement parameter validation
- Part of libpq's internal validation system for safe parameter metadata access
- Static function, not exposed in the public libpq API
- Works with PGresult structures that contain parameter information from prepared statements