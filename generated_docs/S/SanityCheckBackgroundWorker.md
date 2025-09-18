# SanityCheckBackgroundWorker

## Location
src/backend/postmaster/bgworker.c: 637 - 708

## Overview
Validates the configuration of a BackgroundWorker structure and reports errors or warnings based on invalid parameters.

## Definition


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