# CheckpointerShmemStruct

## Location
[src/backend/postmaster/checkpointer.c:126-130](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/checkpointer.c#L126-L130)

## Overview
CheckpointerShmemStruct is the main shared memory structure that coordinates checkpointer process operations and manages synchronization requests between PostgreSQL backends and the checkpointer background worker.

## Definition

```c
typedef struct
{
	pid_t		checkpointer_pid;	/* PID (0 if not started) */

	slock_t		ckpt_lck;		/* protects all the ckpt_* fields */

	int			ckpt_started;	/* advances when checkpoint starts */
	int			ckpt_done;		/* advances when checkpoint done */
	int			ckpt_failed;	/* advances when checkpoint fails */

	int			ckpt_flags;		/* checkpoint flags, as defined in xlog.h */

	ConditionVariable start_cv; /* signaled when ckpt_started advances */
	ConditionVariable done_cv;	/* signaled when ckpt_done advances */

	int			num_requests;	/* current # of requests */
	int			max_requests;	/* allocated array size */
	CheckpointerRequest requests[FLEXIBLE_ARRAY_MEMBER];
} CheckpointerShmemStruct;
```
## Detailed Description
CheckpointerShmemStruct serves as the central coordination point between PostgreSQL backend processes and the checkpointer background worker. It maintains the process ID of the checkpointer, tracks checkpoint operation state, and manages a queue of synchronization requests that need to be processed during checkpoints.

The structure uses condition variables and counters to coordinate checkpoint operations, allowing backends to wait for checkpoints to complete and enabling the checkpointer to signal progress. The flexible array member holds pending sync requests that accumulate between checkpoints and are processed to ensure data durability.

## Parameters / Member Variables
- `checkpointer_pid`: Process ID of the checkpointer background worker (0 if not running)
- `ckpt_lck`: Spinlock that protects all checkpoint-related fields from concurrent access
- `ckpt_started`: Counter that advances each time a checkpoint operation begins
- `ckpt_done`: Counter that advances each time a checkpoint operation completes successfully
- `ckpt_failed`: Counter that advances each time a checkpoint operation fails
- `ckpt_flags`: Flags controlling checkpoint behavior (defined in xlog.h)
- `start_cv`: Condition variable signaled when ckpt_started counter advances
- `done_cv`: Condition variable signaled when ckpt_done counter advances
- `num_requests`: Current number of pending sync requests in the requests array
- `max_requests`: Maximum capacity of the requests array (allocated size)
- `requests[FLEXIBLE_ARRAY_MEMBER]`: Flexible array of CheckpointerRequest structures containing pending sync operations
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