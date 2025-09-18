# AutoVacuumShmemStruct

## Location
src/backend/postmaster/autovacuum.c: 297 - 315

## Overview
AutoVacuumShmemStruct is the main shared memory structure for the PostgreSQL autovacuum system, containing all coordination data between the autovacuum launcher and worker processes.

## Definition
```c
typedef struct
{
	sig_atomic_t av_signal[AutoVacNumSignals];
	pid_t		av_launcherpid;
	dlist_head	av_freeWorkers;
	dlist_head	av_runningWorkers;
	WorkerInfo	av_startingWorker;
	AutoVacuumWorkItem av_workItems[NUM_WORKITEMS];
	pg_atomic_uint32 av_nworkersForBalance;
} AutoVacuumShmemStruct;
```

## Detailed Description
AutoVacuumShmemStruct serves as the central coordination structure for PostgreSQL's autovacuum system in shared memory. This structure manages the state of autovacuum workers, tracks work items, handles inter-process signaling, and coordinates resource balancing between workers. It is the primary mechanism through which the autovacuum launcher process manages and coordinates with autovacuum worker processes.

The structure is designed to handle concurrent access from multiple processes while maintaining consistency. Most fields are protected by the AutovacuumLock, with specific exceptions for signal handling and certain worker list operations that require lock-free access patterns.

## Parameters / Member Variables
- `av_signal`: Atomic signal array for inter-process communication, sized by AutoVacNumSignals enum values
- `av_launcherpid`: Process ID of the current autovacuum launcher process
- `av_freeWorkers`: Doubly-linked list head for available WorkerInfo structures
- `av_runningWorkers`: Doubly-linked list head for currently active WorkerInfo structures  
- `av_startingWorker`: Pointer to WorkerInfo structure currently being started up
- `av_workItems`: Fixed-size array of work items (NUM_WORKITEMS = 256) for tracking vacuum tasks
- `av_nworkersForBalance`: Atomic counter tracking number of workers for cost limit balancing calculations

## Dependencies
- Functions called/Symbols referenced:
  - AutoVacNumSignals (enum defining signal types)
  - WorkerInfo (worker process information structure)
  - AutoVacuumWorkItem (work item structure for vacuum tasks)
  - NUM_WORKITEMS (constant defining work item array size)
  - dlist_head (doubly-linked list infrastructure)
  - pg_atomic_uint32 (atomic integer type)
- Called from (representative examples):
  - AutoVacuumShmemSize (calculates shared memory size requirements)
  - AutoVacuumShmemInit (initializes the shared memory structure)

## Notes and Other Information
- Located at src/backend/postmaster/autovacuum.c:288-297
- This structure is allocated in PostgreSQL shared memory segment and accessed by multiple processes
- Protected primarily by AutovacuumLock, except for av_signal which uses atomic operations
- The av_startingWorker field uses a special protocol where it's cleared by the worker itself once startup is complete
- Work item array provides a bounded queue mechanism for scheduling autovacuum tasks
- The structure is fundamental to PostgreSQL's automatic maintenance system for preventing transaction ID wraparound and managing table bloat