# DebugParallelMode

## Location
src/include/optimizer/optimizer.h: 109 - 185

## Overview
DebugParallelMode is an enumeration type that defines the possible values for the `debug_parallel_query` GUC parameter, which controls how PostgreSQL forces the use of parallel query execution for testing and debugging purposes.

## Definition
```c
typedef enum
{
    DEBUG_PARALLEL_OFF,
    DEBUG_PARALLEL_ON,
    DEBUG_PARALLEL_REGRESS,
} DebugParallelMode;
```

## Detailed Description
The DebugParallelMode enum is used to configure the `debug_parallel_query` GUC (Grand Unified Configuration) parameter, which forces the PostgreSQL planner to generate parallel query plans even when they might not be optimal. This is primarily a developer and testing tool that helps validate the parallel query infrastructure by artificially creating scenarios where parallel execution occurs.

The enum works in conjunction with the `debug_parallel_query` GUC parameter to control parallel query behavior. When enabled, it forces the planner to generate plans that contain nodes performing tuple communication between workers and the main process, which is useful for testing parallel query functionality.

## Parameters / Member Variables
- `DEBUG_PARALLEL_OFF`: Disables forced parallel query execution (default behavior)
- `DEBUG_PARALLEL_ON`: Forces parallel query execution when possible, making parallel operations visible in query plans
- `DEBUG_PARALLEL_REGRESS`: Forces parallel query execution but makes gather nodes "invisible" in EXPLAIN output for regression test consistency

## Dependencies
- Functions called/Symbols referenced:
  - Used by the GUC system in `guc_tables.c`
  - Referenced in planner logic in `planner.c` and `planmain.c`
  - Used in parallel execution control in `parallel.c`

- Called from (representative examples):
  - `debug_parallel_query` GUC variable initialization at src/backend/optimizer/plan/planner.c:68
  - Planner decision logic in `query_planner()` and `create_gather_plan()`
  - Parallel execution control in `ParallelQueryMain()`

## Notes and Other Information
- This is primarily a debugging and testing facility, not intended for production use
- The `DEBUG_PARALLEL_REGRESS` mode is specifically designed for regression tests where consistent EXPLAIN output is required
- The enum is defined in the optimizer header file but affects multiple subsystems including the planner, executor, and parallel worker management
- When `DEBUG_PARALLEL_ON` is used, it can significantly impact query performance since it forces parallelization even when sequential execution would be more efficient
- The GUC parameter can be set at the user session level (PGC_USERSET) and is marked with GUC_NOT_IN_SAMPLE | GUC_EXPLAIN flags