# T_WorkerStatus

## Location
[src/bin/pg_dump/parallel.c:81-82](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/parallel.c#L81-L82)

## Overview
Enum type that defines the possible status states for parallel worker processes in PostgreSQL's pg_dump utility.

## Definition


## Detailed Description
T_WorkerStatus is an enumeration that tracks the lifecycle states of worker processes in pg_dump's parallel dumping functionality. This enum provides a clear state machine for managing worker processes, allowing the leader process to understand what each worker is currently doing and coordinate work distribution accordingly. The enum is used in conjunction with the WORKER_IS_RUNNING macro to determine if a worker is available for new tasks.

## Parameters / Member Variables
-  (0): Initial state indicating the worker process has not yet been started
- : Worker process is running but currently idle and available for new work
- : Worker process is actively executing a task
- : Worker process has completed execution and terminated

## Dependencies
- Functions called/Symbols referenced:
  - None (this is a basic enum definition)
- Called from (representative examples):
  - [ParallelSlot](../P/ParallelSlot.md) (as workerStatus member)
  - WORKER_IS_RUNNING macro

## Notes and Other Information
- The enum values are ordered to reflect the typical lifecycle progression of a worker process
- WORKER_IS_RUNNING macro uses this enum to check if a worker is in an active state (WRKR_IDLE or WRKR_WORKING)
- This is part of pg_dump's parallel processing infrastructure, located in src/bin/pg_dump/parallel.c
- The enum provides type safety and readability compared to using raw integer constants for worker states