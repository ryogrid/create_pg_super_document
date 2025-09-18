# IsLogicalParallelApplyWorker

## Location
src/backend/replication/logical/worker.c: 4821 - 4830

## Overview
IsLogicalParallelApplyWorker is a specialized utility function that determines whether the current process is specifically a logical replication parallel apply worker, as distinct from other types of logical replication workers.

## Definition


## Detailed Description
This function provides a more specific check than IsLogicalWorker by identifying whether the current process is operating as a parallel apply worker within the logical replication system. It combines two conditions: first verifying that the process is any type of logical replication worker, and then confirming that it is specifically a parallel apply worker. Parallel apply workers are used in logical replication to handle changes in parallel for improved performance, as opposed to sequential apply workers or table synchronization workers.

The function returns true only when both conditions are met:
1. The process is a logical replication worker (IsLogicalWorker() returns true)
2. The process is specifically a parallel apply worker (am_parallel_apply_worker() returns true)

## Parameters / Member Variables
This function takes no parameters and returns a boolean value indicating parallel apply worker status.

## Dependencies
- Functions called/Symbols referenced:
  - [IsLogicalWorker](IsLogicalWorker.md) (worker type checking)
  - [am_parallel_apply_worker](../a/am_parallel_apply_worker.md) (parallel apply worker type checking)

- Called from:
  - [mq_putmessage](../m/mq_putmessage.md) (in pqmq.c:168)
  - Referenced in header file logicalworker.h:24

## Notes and Other Information
- This function is more specific than IsLogicalWorker, targeting only parallel apply workers
- Used for conditional logic that should only execute in parallel apply worker contexts
- Part of the public API as declared in logicalworker.h
- Commonly used in message queue operations where parallel workers need special handling
- The parallel apply functionality is part of PostgreSQL's performance optimization for logical replication