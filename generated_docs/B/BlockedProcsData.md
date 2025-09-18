# BlockedProcsData

## Location
src/include/storage/lock.h: 484 - 495

## Overview
BlockedProcsData is a comprehensive data structure that aggregates information about all blocked processes, their associated locks, and wait relationships in PostgreSQL's lock management system.

## Definition
```c
typedef struct BlockedProcsData
{
    BlockedProcData *procs;     /* Array of per-blocked-proc information */
    LockInstanceData *locks;    /* Array of per-PROCLOCK information */
    int            *waiter_pids; /* Array of PIDs of other blocked PGPROCs */
    int             nprocs;     /* # of valid entries in procs[] array */
    int             maxprocs;   /* Allocated length of procs[] array */
    int             nlocks;     /* # of valid entries in locks[] array */
    int             maxlocks;   /* Allocated length of locks[] array */
    int             npids;      /* # of valid entries in waiter_pids[] array */
    int             maxpids;    /* Allocated length of waiter_pids[] array */
} BlockedProcsData;
```

## Detailed Description
BlockedProcsData serves as the central repository for all information related to blocked processes in PostgreSQL. It maintains three separate arrays that work together to provide a complete picture of lock blocking relationships: process information, lock details, and waiter queues. This structure is designed for efficiency in both memory usage and access patterns, using separate arrays with count and capacity tracking for dynamic growth. It supports comprehensive lock analysis, deadlock detection, and provides the foundation for PostgreSQL's lock monitoring and diagnostic capabilities.

## Parameters / Member Variables
- `procs`: Array containing BlockedProcData structures, each representing a blocked process with references to its locks and position in wait queues
- `locks`: Array of LockInstanceData structures containing detailed information about each lock instance relevant to blocked processes
- `waiter_pids`: Array of process IDs representing processes waiting in lock queues, organized to support efficient traversal of wait relationships
- `nprocs`: Current number of valid BlockedProcData entries in the procs array
- `maxprocs`: Total allocated capacity of the procs array, allowing for dynamic growth
- `nlocks`: Current number of valid LockInstanceData entries in the locks array
- `maxlocks`: Total allocated capacity of the locks array
- `npids`: Current number of valid process IDs in the waiter_pids array
- `maxpids`: Total allocated capacity of the waiter_pids array

## Dependencies
- Functions called/Symbols referenced:
  - BlockedProcData
  - LockInstanceData
- Called from (representative examples):
  - PROCLOCK_PRINT
  - GetLockStatusData
  - GetBlockerStatusData
  - GetSingleProcBlockerStatusData
  - pg_blocking_pids
  - LockHashPartitionLockByProc

## Notes and Other Information
- This structure is defined in src/include/storage/lock.h:484-495
- The three-array design (procs, locks, waiter_pids) optimizes memory layout and access patterns for lock analysis
- Dynamic sizing with separate count and capacity fields allows efficient memory management during varying load conditions
- This structure is fundamental to PostgreSQL's deadlock detection algorithms and blocking process identification
- Used extensively by system administration functions and views that report on lock contention and blocking relationships
- The index-based referencing system between arrays provides efficient navigation of complex blocking scenarios