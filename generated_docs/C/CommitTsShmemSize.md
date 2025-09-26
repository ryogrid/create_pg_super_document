# CommitTsShmemSize

## Location
[src/backend/access/transam/commit_ts.c:519-529](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/commit_ts.c#L519-L529)

## Overview
Calculates the total shared memory size required for the CommitTS (commit timestamp) subsystem, including both SLRU buffers and shared control structure.

## Definition
```c
Size CommitTsShmemSize(void)
```

## Detailed Description
This function computes the total amount of shared memory needed for the commit timestamp tracking system. It combines two components:

1. **SLRU buffer space**: Calculated using `SimpleLruShmemSize()` with the buffer count determined by `CommitTsShmemBuffers()`
2. **Control structure space**: The size of the `CommitTimestampShared` structure that contains shared state information

The function is used during PostgreSQL startup to determine how much shared memory to allocate for the commit timestamp subsystem, which tracks when transactions commit.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - CommitTsShmemBuffers
  - SimpleLruShmemSize
  - CommitTimestampShared
- Called from (representative examples):
  - CalculateShmemSize

## Notes and Other Information
- This function is part of PostgreSQL's shared memory initialization process
- The returned `Size` type represents the number of bytes needed
- The commit timestamp feature must be enabled (track_commit_timestamp = on) for this memory to be actively used
- The memory calculated here stores both the actual timestamp data pages and the metadata for managing the SLRU cache