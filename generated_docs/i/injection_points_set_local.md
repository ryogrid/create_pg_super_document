# injection_points_set_local

## Location
src/test/modules/injection_points/injection_points.c: 364 - 385

## Overview
This function enables local injection point tracking for the current process and sets up cleanup mechanisms for process-specific injection points.

## Definition
```c
Datum injection_points_set_local(PG_FUNCTION_ARGS)
```

## Detailed Description
The `injection_points_set_local` function is part of PostgreSQL's injection point testing framework. It enables local injection point tracking by setting the `injection_point_local` flag to true, which allows injection points to be associated with the current process ID for runtime condition checking. The function also initializes the shared memory state if not already done and registers a cleanup callback to automatically remove any process-specific injection points when the process exits.

This function is typically used in testing scenarios where you want injection points to be active only for specific processes, providing better isolation and control during testing.

## Parameters / Member Variables
This function takes no parameters (uses `PG_FUNCTION_ARGS` macro for PostgreSQL function calling convention).

## Dependencies
- Functions called/Symbols referenced:
  - `injection_init_shmem`
  - `before_shmem_exit` 
  - `injection_points_cleanup`
  - `PG_RETURN_VOID`
- Called from (representative examples):
  - `injection_points_wakeup` (src/test/modules/injection_points/injection_points.c:362)

## Notes and Other Information
- Sets the global variable `injection_point_local` to true to enable process-specific injection point conditions
- Ensures shared memory is initialized for the injection points system
- Registers the `injection_points_cleanup` callback to automatically clean up injection points when the process terminates
- Part of the PostgreSQL test infrastructure, located in src/test/modules/injection_points/
- Returns void through the PostgreSQL function interface