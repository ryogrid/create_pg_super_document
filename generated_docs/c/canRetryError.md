# canRetryError

## Location
src/bin/pgbench/pgbench.c: 3225 - 3240

## Overview
Determines whether a specific error status type is eligible for retry in pgbench's error handling mechanism.

## Definition
```c
static bool canRetryError(EStatus estatus)
```

## Detailed Description
This function evaluates an error status value and returns true if the error represents a transient condition that can potentially be resolved by retrying the operation. Currently, it only considers serialization failures and deadlock errors as retryable, as these are temporary conflicts that may succeed on subsequent attempts. Other types of errors (syntax errors, permission issues, etc.) are not considered retryable.

## Parameters / Member Variables
- `estatus`: An EStatus enumeration value representing the categorized error type, typically obtained from getSQLErrorStatus()

## Dependencies
- Functions called/Symbols referenced:
  - EStatus (enumeration type)
  - ESTATUS_SERIALIZATION_ERROR (enum value for serialization failures)
  - ESTATUS_DEADLOCK_ERROR (enum value for deadlock errors)
- Called from (representative examples):
  - readCommandResponse
  - doRetry
  - advanceConnectionState

## Notes and Other Information
- Returns true only for ESTATUS_SERIALIZATION_ERROR and ESTATUS_DEADLOCK_ERROR
- Part of pgbench's intelligent retry logic for handling database concurrency issues
- The function is static with internal linkage within pgbench.c
- Helps distinguish between permanent errors (which should not be retried) and temporary conflicts