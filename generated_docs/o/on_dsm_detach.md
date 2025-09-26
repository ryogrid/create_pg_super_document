# on_dsm_detach

## Location
src/backend/storage/ipc/dsm.c: 1132 - 1146

## Overview
Registers a callback function that will be executed when a dynamic shared memory segment is detached from the current process.

## Definition
```c
void on_dsm_detach(dsm_segment *seg, on_dsm_detach_callback function, Datum arg)
```

## Detailed Description
This function allows processes to register cleanup callbacks that will be invoked automatically when a dynamic shared memory (DSM) segment is detached. This is essential for proper resource management and cleanup in PostgreSQL's parallel processing infrastructure.

The function allocates memory for a callback structure in the TopMemoryContext (ensuring it persists for the lifetime of the process) and adds it to a singly-linked list of callbacks associated with the segment. When the segment is eventually detached, all registered callbacks will be executed in reverse order of registration (since callbacks are added to the head of the list).

This mechanism is widely used throughout PostgreSQL's shared memory facilities to ensure proper cleanup of resources like shared message queues, file sets, and memory allocators when processes detach from shared memory segments.

## Parameters / Member Variables
- `seg`: Pointer to the dsm_segment structure for which to register the callback
- `function`: The callback function to be called on detachment (of type on_dsm_detach_callback)
- `arg`: A Datum argument that will be passed to the callback function when invoked

## Dependencies
- Functions called/Symbols referenced:
  - dsm_segment (structure type)
  - dsm_segment_detach_callback (structure type)
  - MemoryContextAlloc (memory allocation function)
  - slist_push_head (linked list manipulation)
  - TopMemoryContext (global memory context)
- Called from (representative examples):
  - pq_redirect_to_shm_mq
  - SharedFileSetInit
  - SharedFileSetAttach
  - shm_mq_attach
  - SharedRecordTypmodRegistryInit
  - dsa_create_ext
  - dsa_attach

## Notes and Other Information
- Callbacks are executed in LIFO order (last registered, first executed) due to the use of slist_push_head
- The callback mechanism ensures proper cleanup even in error scenarios
- Memory for callbacks is allocated in TopMemoryContext to ensure it persists until process termination
- This is a fundamental part of PostgreSQL's resource management for shared memory segments
- Commonly used by higher-level shared memory facilities like shared message queues, shared file sets, and dynamic shared memory allocators
- The Datum argument allows passing context-specific data to the callback function