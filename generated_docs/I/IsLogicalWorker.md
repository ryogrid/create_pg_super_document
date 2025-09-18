# IsLogicalWorker

## Location
[src/backend/replication/logical/worker.c:4812-4820](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L4812-L4820)

## Overview
IsLogicalWorker is a simple utility function that determines whether the current process is a logical replication worker by checking the global worker state.

## Definition


## Detailed Description
This function provides a straightforward way to identify if the current process is operating as a logical replication worker. It performs this check by examining the global variable MyLogicalRepWorker, which is non-NULL when the process is functioning as a logical replication worker (either an apply worker or a table synchronization worker). The function is commonly used for conditional logic that should only execute within the context of logical replication processes.

## Parameters / Member Variables
This function takes no parameters and returns a boolean value indicating worker status.

## Dependencies
- Functions called/Symbols referenced:
  - MyLogicalRepWorker (global variable check)

- Called from:
  - [IsLogicalParallelApplyWorker](IsLogicalParallelApplyWorker.md) (in worker.c:4823)
  - ProcessInterrupts (in postgres.c:3294)
  - Referenced in header file logicalworker.h:23

## Notes and Other Information
- This is a lightweight check function with minimal overhead
- The function relies on the global variable MyLogicalRepWorker being properly initialized
- Returns true for both apply workers and table synchronization workers
- Used by other functions to conditionally execute logic specific to logical replication contexts
- Part of the public API as declared in logicalworker.h