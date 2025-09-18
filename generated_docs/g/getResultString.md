# getResultString

## Location
src/bin/pgbench/pgbench.c: 4530 - 4560

## Overview
Returns a string constant representing the result status of a transaction that was not successfully processed, providing detailed failure information when requested.

## Definition
```c
static const char *getResultString(bool skipped, EStatus estatus)
```

## Detailed Description
This function generates human-readable status strings for failed or skipped transactions in pgbench. It handles different types of transaction outcomes based on whether the transaction was skipped or failed due to specific errors. When detailed failure reporting is enabled (`failures_detailed` flag), it provides specific error type information (serialization or deadlock). Otherwise, it returns generic status strings. The function is primarily used for logging and reporting transaction outcomes.

## Parameters / Member Variables
- `skipped`: Boolean flag indicating whether the transaction was skipped rather than failed
- `estatus`: Enumerated status value indicating the specific type of error that occurred (EStatus type)

## Dependencies
- Functions called/Symbols referenced:
  - EStatus (enumeration type)
  - ESTATUS_SERIALIZATION_ERROR (enum constant)
  - ESTATUS_DEADLOCK_ERROR (enum constant)
  - pg_fatal (error reporting function)
- Called from (representative examples):
  - doLog (at src/bin/pgbench/pgbench.c:4662)

## Notes and Other Information
- This is a static function, only accessible within pgbench.c
- Returns different strings based on the `failures_detailed` global variable setting
- Possible return values: "skipped", "serialization", "deadlock", "failed"
- Contains error handling for unexpected status values using pg_fatal()
- Used in conjunction with transaction logging to provide meaningful status descriptions