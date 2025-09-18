# LockData

## Location
[src/include/storage/lock.h:465-469](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/lock.h#L465-L469)

## Overview
LockData is a structure that encapsulates an array of lock instance information used for reporting and analyzing lock status in PostgreSQL.

## Definition
```c
typedef struct LockData
{
    int         nelements;      /* The length of the array */
    LockInstanceData *locks;    /* Array of per-PROCLOCK information */
} LockData;
```

## Detailed Description
LockData serves as a container structure for collecting and passing lock information within PostgreSQL's lock management system. It provides a standardized way to bundle lock instance data along with the count of elements, making it easier to work with collections of lock information when analyzing or reporting lock status. This structure is primarily used by functions that need to gather and return information about currently held locks in the system.

## Parameters / Member Variables
- `nelements`: The number of valid entries in the locks array, indicating how many lock instances are contained in this structure
- `locks`: Pointer to an array of LockInstanceData structures, each containing detailed information about a specific process-lock combination (PROCLOCK)

## Dependencies
- Functions called/Symbols referenced:
  - [LockInstanceData](LockInstanceData.md)
- Called from (representative examples):
  - [LockShmemSize](LockShmemSize.md)
  - [GetLockStatusData](../G/GetLockStatusData.md)
  - [pg_lock_status](../p/pg_lock_status.md)
  - LockHashPartitionLockByProc

## Notes and Other Information
- This structure is defined in src/include/storage/lock.h:465-469
- It is used primarily for lock status reporting and debugging purposes
- The structure provides a clean interface for functions that need to return collections of lock information
- Memory management for the locks array is handled by the calling functions
- This structure is part of PostgreSQL's comprehensive lock monitoring and diagnostic infrastructure