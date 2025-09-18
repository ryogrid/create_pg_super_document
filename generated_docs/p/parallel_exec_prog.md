# parallel_exec_prog

## Location
src/bin/pg_upgrade/parallel.c: 62 - 152

## Overview
Executes a command in parallel mode as part of PostgreSQL's pg_upgrade utility, providing concurrent execution capabilities for improved performance during database upgrades.

## Definition


## Detailed Description
This function provides parallel execution capabilities for pg_upgrade operations. It has the same API as exec_prog but adds parallel execution support. The function manages a pool of worker processes (on Unix) or threads (on Windows) to execute commands concurrently, significantly improving performance during database upgrade operations.

When parallel jobs are disabled (user_opts.jobs <= 1), it falls back to sequential execution using exec_prog. In parallel mode, it manages process/thread lifecycle, including forking new processes on Unix systems or creating new threads on Windows, and handles job scheduling to respect the maximum number of concurrent jobs.

The function ensures proper stdio state before forking and includes comprehensive error handling for process/thread creation failures.

## Parameters / Member Variables
- : Path to the main log file where command output will be written
- : Optional path to an additional log file (can be NULL)
- : Printf-style format string for the command to execute
- : Variable arguments corresponding to the format string

## Dependencies
- Functions called/Symbols referenced:
  - exec_prog
  - reap_child
  - win32_exec_prog (Windows only)
  - vsnprintf
  - pg_malloc
  - pg_malloc0
  - pg_free
  - pg_strdup
  - fork (Unix)
  - _beginthreadex (Windows)
- Called from (representative examples):
  - generate_old_dump
  - create_new_objects

## Notes and Other Information
- Platform-specific implementation: Uses fork() on Unix systems and _beginthreadex() on Windows
- Thread safety: On Windows, maintains thread-safe argument structures and handles
- Memory management: Carefully manages memory allocation for thread arguments on Windows
- Error handling: Must throw errors rather than return error status due to parallel execution nature
- Job control: Respects user_opts.jobs limit and implements job harvesting through reap_child()
- Performance optimization: Significantly improves pg_upgrade performance by allowing concurrent execution of multiple operations