# pg_rusage_init

## Location
[src/backend/utils/misc/pg_rusage.c:27-39](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/pg_rusage.c#L27-L39)

## Overview
Initializes a resource usage snapshot by capturing current process resource usage statistics and wall clock time.

## Definition

```c
void
pg_rusage_init(PGRUsage *ru0)
```
## Detailed Description
The  function captures the current state of system resource usage for the calling process. It stores both the CPU time consumption (user and system time) via the  system call and the current wall-clock time via  into a  structure. This snapshot serves as a baseline measurement point that can later be compared with another snapshot using  to calculate elapsed time and resource consumption.

This function is commonly used at the beginning of performance-critical operations to establish a timing baseline for later performance reporting and analysis.

## Parameters / Member Variables
- : Pointer to a  structure where the current resource usage snapshot will be stored. The structure contains:
  - :  for wall-clock time (filled by )
  - :  for process resource usage statistics (filled by )

## Dependencies
- Functions called/Symbols referenced:
  -  - System call to get process resource usage
  -  - System call to get current wall-clock time
  -  - Constant specifying to get resource usage for calling process
  -  - Structure type for storing usage snapshots
- Called from (representative examples):
  -  - For vacuum operation timing
  -  - For WAL recovery timing
  -  - For index rebuild timing
  -  - For table analysis timing
  -  - For sort operation timing
  -  - To get current snapshot for comparison

## Notes and Other Information
- This function must be paired with  to calculate meaningful timing and resource usage deltas
- The function uses system calls that are available on POSIX-compliant systems
- Resource usage includes CPU time (user and system), memory usage, and I/O statistics as provided by the underlying OS
- Wall-clock time is measured separately to distinguish between CPU time and real elapsed time
- Commonly used pattern: call  before an operation, then  after to report performance metrics

## Simplified Source

```c
// Simplified version of pg_rusage_init
void pg_rusage_init(PGRUsage *ru0) {
    // Capture current process resource usage (CPU time, memory, I/O stats)
    getrusage(RUSAGE_SELF, &ru0->ru);

    // Capture current wall-clock time
    gettimeofday(&ru0->tv, NULL);
}
```

Key simplifications made:
- Added explanatory comments for the two main operations
- The function is already quite simple, so minimal changes were needed
- Preserved the essential functionality: capturing resource usage and wall time