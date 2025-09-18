# rusage

## Location
[src/include/port/win32/sys/resource.h:12-20](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/win32/sys/resource.h#L12-L20)

## Overview
A portable structure that represents resource usage information for a process, specifically tracking CPU time consumption in both user and system modes.

## Definition


## Detailed Description
The  structure is PostgreSQL's Windows-compatible implementation of the standard Unix  structure. It provides a simplified subset of the full Unix resource usage interface, focusing on the most essential timing information needed for performance monitoring and debugging. This structure is primarily used on Windows platforms where the native  functionality is not available.

The structure captures two fundamental timing metrics:
- **User time**: CPU time spent executing user-mode code
- **System time**: CPU time spent in kernel/system calls

This implementation is part of PostgreSQL's portability layer, allowing consistent resource usage tracking across different operating systems. The structure works in conjunction with the  function implementation to provide Unix-like resource monitoring capabilities on Windows.

## Parameters / Member Variables
- : A  structure containing the total amount of time spent executing in user mode, broken down into seconds and microseconds
- : A  structure containing the total amount of time spent executing in system/kernel mode, broken down into seconds and microseconds

## Dependencies
- Functions called/Symbols referenced:
  - timeval (structure type)
  - getrusage (function that populates this structure)
- Called from (representative examples):
  - get_stack_depth_rlimit (in src/backend/tcop/postgres.c:5076)
  - ShowUsage (in src/backend/tcop/postgres.c:5093)
  - PGRUsage (used as member in src/include/utils/pg_rusage.h:25)
  - getrusage (in src/port/win32getrusage.c - multiple locations)

## Notes and Other Information
- This is a Windows-specific implementation found in 
- The structure is a simplified version of the full Unix  structure, containing only the most essential timing information
- Used primarily for performance monitoring, debugging, and resource usage reporting in PostgreSQL
- The companion  function in  populates this structure using Windows-specific APIs like 
- Unlike the full Unix  structure, this implementation does not include memory usage statistics or other resource metrics
- The structure is integrated into PostgreSQL's performance monitoring infrastructure through the  wrapper structure