# check_param_number

## Location
[src/interfaces/libpq/fe-exec.c:3548-3566](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L3548-L3566)

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
  - [pqInternalNotice](../p/pqInternalNotice.md) (for error reporting)
- Called from (representative examples):
  - [PQparamtype](../P/PQparamtype.md)

## Notes and Other Information
- Returns true if the parameter number is valid, false otherwise
- If res is NULL, returns false immediately without error message
- Uses 0-based indexing for parameter numbers (0 to numParameters-1)
- Specifically designed for prepared statement parameter validation
- Part of libpq's internal validation system for safe parameter metadata access
- Static function, not exposed in the public libpq API
- Works with PGresult structures that contain parameter information from prepared statements

## Simplified Source

```c
static int check_param_number(const PGresult *res, int param_num) {
    // Fail fast if no result object
    if (!res)
        return false;

    // Validate parameter number is within range
    if (param_num < 0 || param_num >= res->numParameters) {
        pqInternalNotice(&res->noticeHooks,
                         "parameter number %d is out of range 0..%d",
                         param_num, res->numParameters - 1);
        return false;
    }

    return true;
}
```