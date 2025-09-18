# doRetry

## Location
src/bin/pgbench/pgbench.c: 3428 - 3473

## Overview
Determines whether a failed transaction can be retried based on error type, retry limits, latency constraints, and benchmark duration.

## Definition
```c
static bool doRetry(CState *st, pg_time_usec_t *now)
```

## Detailed Description
This function implements pgbench's retry policy for failed transactions. It performs multiple checks to determine if a retry is appropriate: first verifying that the error type is retryable (using canRetryError), then checking various limits including maximum retry attempts, per-transaction latency limits, and overall benchmark duration. The function ensures that retries are only attempted for transient errors like serialization failures and deadlocks, while respecting configured constraints to prevent infinite retry loops or excessive benchmark runtime.

## Parameters / Member Variables
- `st`: Pointer to CState structure containing client state, error status, retry count, and transaction timing information
- `now`: Pointer to pg_time_usec_t for current time (updated lazily when needed for latency checking)

## Dependencies
- Functions called/Symbols referenced:
  - [CState](../C/CState.md) (client state structure)
  - pg_time_usec_t (time type definition)
  - ESTATUS_NO_ERROR (error status constant)
  - [canRetryError](../c/canRetryError.md) (check if error type is retryable)
  - [pg_time_now_lazy](../p/pg_time_now_lazy.md) (get current time when needed)
  - Global variables: max_tries, latency_limit, duration, timer_exceeded
- Called from (representative examples):
  - [advanceConnectionState](../a/advanceConnectionState.md) (multiple call sites)

## Notes and Other Information
- Returns true if retry is allowed, false if any constraint prevents retrying
- Requires at least one limiting factor (max_tries, latency_limit, or duration) to be configured
- Only processes errors already determined to have retry potential by canRetryError()
- Implements lazy time evaluation to avoid unnecessary system calls when latency_limit is not set
- Part of pgbench's comprehensive error handling and retry mechanism
- The function is static with internal linkage within pgbench.c
- Prevents infinite retry loops through multiple constraint checks