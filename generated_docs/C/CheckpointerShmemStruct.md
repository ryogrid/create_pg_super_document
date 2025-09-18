# CheckpointerShmemStruct

## Location
src/backend/postmaster/checkpointer.c: 126 - 130

## Overview
CheckpointerShmemStruct is the main shared memory structure that coordinates checkpointer process operations and manages synchronization requests between PostgreSQL backends and the checkpointer background worker.

## Definition


## Detailed Description
CheckpointerShmemStruct serves as the central coordination point between PostgreSQL backend processes and the checkpointer background worker. It maintains the process ID of the checkpointer, tracks checkpoint operation state, and manages a queue of synchronization requests that need to be processed during checkpoints.

The structure uses condition variables and counters to coordinate checkpoint operations, allowing backends to wait for checkpoints to complete and enabling the checkpointer to signal progress. The flexible array member holds pending sync requests that accumulate between checkpoints and are processed to ensure data durability.

## Parameters / Member Variables
- : Process ID of the checkpointer background worker (0 if not running)
- : Spinlock that protects all checkpoint-related fields from concurrent access
- : Counter that advances each time a checkpoint operation begins
- : Counter that advances each time a checkpoint operation completes successfully
- : Counter that advances each time a checkpoint operation fails
- : Flags controlling checkpoint behavior (defined in xlog.h)
- : Condition variable signaled when ckpt_started counter advances
- : Condition variable signaled when ckpt_done counter advances
- : Current number of pending sync requests in the requests array
- : Maximum capacity of the requests array (allocated size)
- : Flexible array of CheckpointerRequest structures containing pending sync operations

## Dependencies
- Functions called/Symbols referenced:
  - pid_t
  - [slock_t](../s/slock_t.md)
  - ConditionVariable
  - [CheckpointerRequest](CheckpointerRequest.md)
  - FLEXIBLE_ARRAY_MEMBER
- Used by:
  - [CheckpointerMain](CheckpointerMain.md)
  - [ImmediateCheckpointRequested](../I/ImmediateCheckpointRequested.md)
  - [CheckpointerShmemSize](CheckpointerShmemSize.md)
  - [CheckpointerShmemInit](CheckpointerShmemInit.md)

## Notes and Other Information
- The structure is allocated in shared memory during PostgreSQL startup via CheckpointerShmemInit()
- Size calculation includes space for up to Min(NBuffers, MAX_CHECKPOINT_REQUESTS) sync requests
- The condition variables enable efficient waiting for checkpoint completion without busy polling
- Spinlock protection ensures atomic updates to checkpoint state counters
- This is a critical component of PostgreSQL's crash recovery and data durability subsystem
- The flexible array design allows the structure size to be determined at runtime based on buffer pool size