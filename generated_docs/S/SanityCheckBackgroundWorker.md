# SanityCheckBackgroundWorker

## Location
[src/backend/postmaster/bgworker.c:637-708](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/bgworker.c#L637-L708)

## Overview
Validates the configuration of a BackgroundWorker structure and reports errors or warnings based on invalid parameters.

## Definition

```c
static bool
SanityCheckBackgroundWorker(BackgroundWorker *worker, int elevel)
```
## Detailed Description
This function performs comprehensive validation of a BackgroundWorker configuration before it can be registered with the system. It checks various aspects of the worker configuration including flags, restart intervals, database connection requirements, and parallel worker constraints. The function uses PostgreSQL's error reporting system to communicate validation failures and can be configured to report at different error levels (WARNING, ERROR, etc.).

Key validation checks include:
- Ensuring shared memory access is requested (now mandatory)
- Validating database connection requirements against start time constraints
- Checking restart interval bounds and validity
- Enforcing restrictions on parallel workers regarding restart policies
- Setting default bgw_type if not specified

## Parameters / Member Variables
- : Pointer to the BackgroundWorker structure to be validated
- : Error level for reporting validation failures (e.g., WARNING, ERROR, FATAL)

## Dependencies
- Functions called/Symbols referenced:
  - ereport (error reporting)
  - [errcode](../e/errcode.md) (error code generation)
  - [errmsg](../e/errmsg.md) (error message formatting)
  - strcmp (string comparison)
  - strcpy (string copy)
- Constants referenced:
  - BGWORKER_SHMEM_ACCESS
  - BGWORKER_BACKEND_DATABASE_CONNECTION
  - BgWorkerStart_PostmasterStart
  - BGW_NEVER_RESTART
  - USECS_PER_DAY
  - BGWORKER_CLASS_PARALLEL
- Called from:
  - [RegisterBackgroundWorker](../R/RegisterBackgroundWorker.md)
  - [RegisterDynamicBackgroundWorker](../R/RegisterDynamicBackgroundWorker.md)

## Notes and Other Information
- This is a static function internal to bgworker.c, not exposed in public APIs
- The function enforces the requirement that all background workers must have shared memory access (a change from earlier PostgreSQL versions)
- Parallel workers have special restrictions and cannot be configured for automatic restart
- If bgw_type is empty, it defaults to the value of bgw_name
- Returns true if validation passes, false otherwise (unless elevel >= ERROR, in which case it may not return on failure)

## Simplified Source

```c
// Simplified version of SanityCheckBackgroundWorker
static bool SanityCheckBackgroundWorker(BackgroundWorker *worker, int elevel) {
    // Check 1: Shared memory access is mandatory
    if (!(worker->bgw_flags & BGWORKER_SHMEM_ACCESS)) {
        ereport(elevel, "background workers without shared memory access are not supported");
        return false;
    }

    // Check 2: Database connection workers cannot start at postmaster start
    if (worker->bgw_flags & BGWORKER_BACKEND_DATABASE_CONNECTION) {
        if (worker->bgw_start_time == BgWorkerStart_PostmasterStart) {
            ereport(elevel, "cannot request database access if starting at postmaster start");
            return false;
        }
    }

    // Check 3: Validate restart interval bounds
    if ((worker->bgw_restart_time < 0 && worker->bgw_restart_time != BGW_NEVER_RESTART) ||
        (worker->bgw_restart_time > USECS_PER_DAY / 1000)) {
        ereport(elevel, "invalid restart interval");
        return false;
    }

    // Check 4: Parallel workers cannot be configured for restart
    if (worker->bgw_restart_time != BGW_NEVER_RESTART &&
        (worker->bgw_flags & BGWORKER_CLASS_PARALLEL) != 0) {
        ereport(elevel, "parallel workers may not be configured for restart");
        return false;
    }

    // Check 5: Set default bgw_type if not specified
    if (strcmp(worker->bgw_type, "") == 0) {
        strcpy(worker->bgw_type, worker->bgw_name);
    }

    return true;
}
```

Key simplifications made:
- Simplified error reporting calls to focus on the core message
- Added numbered comments for each validation check
- Condensed the logic flow while preserving all essential validations
- Removed detailed error code specifications for clarity
- Maintained the exact same validation logic and return behavior