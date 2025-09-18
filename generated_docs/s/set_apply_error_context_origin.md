# set_apply_error_context_origin

## Location
src/backend/replication/logical/worker.c: 5111 - 5125

## Overview
This function allocates and stores an origin name in a long-lived memory context for use in error context messages during logical replication apply operations.

## Definition
void set_apply_error_context_origin(char *originname)

## Detailed Description
set_apply_error_context_origin is a utility function used in PostgreSQL's logical replication system to set up error context information. It takes an origin name string and stores a copy of it in the ApplyContext memory context, which is a long-lived context that persists throughout the logical replication apply worker's lifetime. This stored origin name is later used in error messages to provide better diagnostic information when errors occur during the application of replicated changes. The function ensures that the origin name remains available for error reporting even after the original string might have been deallocated.

## Parameters / Member Variables
- `originname`: A null-terminated string containing the name of the replication origin to be stored for error context

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextStrdup
  - ApplyContext (global memory context)
  - apply_error_callback_arg (global structure to store error context)
- Called from (representative examples):
  - ParallelApplyWorkerMain (src/backend/replication/logical/applyparallelworker.c:968)
  - run_tablesync_worker (src/backend/replication/logical/tablesync.c:1725)
  - run_apply_worker (src/backend/replication/logical/worker.c:4532)

## Notes and Other Information
- The function is designed to be called during the initialization phase of logical replication workers
- Uses MemoryContextStrdup to ensure the string copy persists in the ApplyContext memory context
- The stored origin name becomes part of the error callback argument structure for better error diagnostics
- This is part of PostgreSQL's error handling infrastructure for logical replication, helping administrators identify which replication origin was involved when errors occur
- The ApplyContext is specifically chosen as it has the appropriate lifetime for the apply worker process