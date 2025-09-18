# PQnparams

## Location
src/interfaces/libpq/fe-exec.c: 3915 - 3925

## Overview
PQnparams returns the number of input parameters that a prepared statement expects.

## Definition
```c
int PQnparams(const PGresult *res)
```

## Detailed Description
PQnparams retrieves the count of parameters that a prepared statement was designed to accept. This function is typically used after executing a PREPARE statement or calling PQprepare() to understand how many parameters need to be provided when executing the prepared statement with PQexecPrepared() or PQexecParams().

The function is straightforward - it checks if the PGresult pointer is valid and returns the numParameters field from the result structure. For non-prepared statements or invalid results, it returns 0.

This function is essential for prepared statement introspection, allowing applications to validate that they are providing the correct number of parameters before execution.

## Parameters / Member Variables
- `res`: Pointer to the PGresult structure, typically from a PREPARE command or PQdescribePrepared()

## Dependencies
- Functions called/Symbols referenced:
  - (None - accesses res->numParameters directly)
- Called from (representative examples):
  - (Limited direct usage found in codebase - primarily used by client applications)

## Notes and Other Information
- Returns 0 if the PGresult pointer is NULL
- Only meaningful for results from PREPARE statements or PQdescribePrepared() calls
- The returned count indicates how many parameters must be provided to PQexecPrepared() or similar functions
- Essential for prepared statement parameter validation in client applications
- Part of the prepared statement introspection API alongside PQparamtype()