# reap_child

## Location
src/bin/pg_upgrade/parallel.c: 278 - 341

## Overview
Collects status from completed worker child processes or threads, managing the lifecycle of parallel jobs in PostgreSQL's pg_upgrade utility.

## Definition


## Detailed Description
This function is responsible for managing the lifecycle of parallel worker processes (Unix) or threads (Windows) in pg_upgrade. It collects exit status from completed workers, handles cleanup of system resources, and maintains the parallel job count. The function provides both blocking and non-blocking modes of operation.

On Unix systems, it uses waitpid() to collect child process exit status and handles process cleanup. On Windows, it uses WaitForMultipleObjects() to wait for thread completion, retrieves thread exit codes, and manages thread handle cleanup to prevent resource leaks.

The function includes sophisticated thread/process management on Windows, including array compaction to maintain efficient slot usage when workers complete out of order.

## Parameters / Member Variables
- : Boolean flag controlling blocking behavior
  - : Block until a child/thread completes (blocking mode)
  - : Return immediately if no completed children available (non-blocking mode)

## Dependencies
- Functions called/Symbols referenced:
  - waitpid (Unix)
  - WaitForMultipleObjects (Windows)
  - GetExitCodeThread (Windows)
  - CloseHandle (Windows)
  - pg_fatal (error reporting)
- Called from (representative examples):
  - parallel_exec_prog
  - parallel_transfer_all_new_dbs
  - generate_old_dump
  - create_new_objects
  - transfer_all_new_tablespaces

## Notes and Other Information
- Platform-specific implementation: Uses waitpid() on Unix and Windows threading APIs on Windows
- Resource management: Prevents handle leaks on Windows by properly closing thread handles
- Error handling: Validates exit status and reports abnormal terminations via pg_fatal()
- Job tracking: Maintains parallel_jobs counter to track active worker count
- Array management: On Windows, implements slot compaction to efficiently reuse thread argument structures
- Performance optimization: Supports both blocking and non-blocking operation modes for flexible job scheduling
- Thread safety: Carefully manages shared thread argument arrays on Windows platforms
- Return value: Returns true if a worker was reaped, false if no workers were available or completed