# pa_set_stream_apply_worker

## Location
src/backend/replication/logical/applyparallelworker.c: 1334 - 1347

## Overview
pa_set_stream_apply_worker is a utility function that caches the parallel apply worker information for use within PostgreSQL's logical replication streaming system.

## Definition
void pa_set_stream_apply_worker(ParallelApplyWorkerInfo *winfo)

## Detailed Description
This function provides a simple mechanism to cache parallel apply worker information in a global variable (stream_apply_worker). It serves as a setter function that stores a reference to the ParallelApplyWorkerInfo structure, making the worker information accessible to other parts of the parallel apply worker subsystem.

The function is straightforward in its operation - it simply assigns the provided worker information pointer to the global stream_apply_worker variable. This cached information is later used by various parts of the streaming logical replication system to access worker details without needing to pass the information through function parameters.

The caching mechanism allows the system to maintain a reference to the current streaming worker context, enabling efficient access to worker-specific information during stream processing operations.

## Parameters / Member Variables
- : Pointer to the ParallelApplyWorkerInfo structure containing information about the parallel apply worker to be cached

## Dependencies
- Functions called/Symbols referenced:
  - [ParallelApplyWorkerInfo](../P/ParallelApplyWorkerInfo.md) (worker information structure)
  - stream_apply_worker (global variable for caching worker info)
- Called from (representative examples):
  - [apply_handle_stream_start](../a/apply_handle_stream_start.md) (multiple locations)
  - [apply_handle_stream_stop](../a/apply_handle_stream_stop.md) (multiple locations)

## Notes and Other Information
- This function is located in src/backend/replication/logical/applyparallelworker.c:1334-1347
- Uses a simple assignment to a global variable for caching worker information
- Part of the streaming logical replication infrastructure that handles parallel processing
- The cached information is typically used during stream start, stop, and processing operations
- Provides a centralized way to access current worker information without parameter passing
- The global variable approach suggests this function is used in contexts where worker information needs to be accessible across multiple function calls within the same execution context