# AutoVacuumShmemInit

## Location
src/backend/postmaster/autovacuum.c: 3319 - 3363

## Overview
AutoVacuumShmemInit allocates and initializes the shared memory structures required for the autovacuum subsystem, setting up worker pools and control data structures.

## Definition


## Detailed Description
This function is responsible for setting up the autovacuum subsystem's shared memory region during PostgreSQL startup. It performs different operations depending on whether it's running in the postmaster process or a child process:

**In the postmaster process (when !IsUnderPostmaster):**
- Allocates shared memory for autovacuum data structures using ShmemInitStruct()
- Initializes the main AutoVacuumShmem control structure
- Sets up two doubly-linked lists for worker management: av_freeWorkers and av_runningWorkers
- Initializes the work items array for tracking autovacuum tasks
- Creates and initializes the worker information pool, adding all workers to the free list
- Initializes atomic variables for load balancing coordination

**In child processes:**
- Simply attaches to the existing shared memory region and verifies it was found

The function sets up a worker pool system where WorkerInfo structures are managed in free and running lists. Each worker has atomic flags for coordination, particularly for load balancing operations. The shared memory layout places the main control structure first, followed by the variable-sized array of worker information structures.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [ShmemInitStruct](../S/ShmemInitStruct.md)() (shared memory allocation)
  - [AutoVacuumShmemSize](AutoVacuumShmemSize.md)() (size calculation)
  - [dlist_init](../d/dlist_init.md)() (doubly-linked list initialization)
  - [dlist_push_head](../d/dlist_push_head.md)() (list manipulation)
  - [pg_atomic_init_flag](../p/pg_atomic_init_flag.md)() (atomic flag initialization)
  - [pg_atomic_init_u32](../p/pg_atomic_init_u32.md)() (atomic counter initialization)
  - memset() (memory zeroing)
  - MAXALIGN() (memory alignment)
- Data structures referenced:
  - [AutoVacuumShmemStruct](AutoVacuumShmemStruct.md) (main control structure)
  - [WorkerInfo](../W/WorkerInfo.md) (worker information structure)
  - [AutoVacuumWorkItem](AutoVacuumWorkItem.md) (work item structure)
- Global variables used:
  - AutoVacuumShmem (global shared memory pointer)
  - autovacuum_max_workers (configuration parameter)
  - NUM_WORKITEMS (work items array size)
  - IsUnderPostmaster (process type flag)
- Called from:
  - [CreateOrAttachShmemStructs](../C/CreateOrAttachShmemStructs.md) (at src/backend/storage/ipc/ipci.c:341)

## Notes and Other Information
- This function is part of PostgreSQL's shared memory initialization sequence
- Only the postmaster process performs the actual initialization; child processes just attach
- The worker pool uses doubly-linked lists for efficient worker allocation and tracking
- Atomic variables are used for coordination between processes, particularly for load balancing
- The shared memory region name is "AutoVacuum Data" for identification purposes
- The function ensures proper memory alignment for the worker array placement
- Located in src/backend/postmaster/autovacuum.c:3319-3363