# AsyncQueueControl

## Location
[src/backend/commands/async.c:281-292](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/async.c#L281-L292)

## Overview
AsyncQueueControl is the main shared memory control structure for PostgreSQL's LISTEN/NOTIFY asynchronous messaging system, coordinating access to the notification queue among multiple backend processes.

## Definition

```c
typedef struct AsyncQueueControl
{
	QueuePosition head;			/* head points to the next free location */
	QueuePosition tail;			/* tail must be <= the queue position of every
								 * listening backend */
	int64		stopPage;		/* oldest unrecycled page; must be <=
								 * tail.page */
	ProcNumber	firstListener;	/* id of first listener, or
								 * INVALID_PROC_NUMBER */
	TimestampTz lastQueueFillWarn;	/* time of last queue-full msg */
	QueueBackendStatus backend[FLEXIBLE_ARRAY_MEMBER];
} AsyncQueueControl;
```
## Detailed Description
AsyncQueueControl serves as the central coordination structure for PostgreSQL's asynchronous notification system. It manages the notification queue's head and tail positions, tracks which pages can be recycled, and maintains information about all listening backends. The structure uses a sophisticated locking protocol with NotifyQueueLock and NotifyQueueTailLock to ensure safe concurrent access. The backend array contains status information for each potentially listening backend, indexed by ProcNumber, and active listeners are linked together for efficient scanning.

## Parameters / Member Variables
- : QueuePosition pointing to the next free location where new notifications can be written
- : QueuePosition that must be less than or equal to the queue position of every listening backend (ensures no notifications are lost)
- : Page number of the oldest unrecycled page, must be less than or equal to tail.page for proper memory management
- : ProcNumber of the first listener in the linked list, or INVALID_PROC_NUMBER if no listeners
- : Timestamp of the last time a queue-full warning was issued to prevent spam
- : Flexible array of QueueBackendStatus structures, one per possible backend process

## Dependencies
- Functions called/Symbols referenced:
  - [QueuePosition](../Q/QueuePosition.md)
  - ProcNumber
  - [QueueBackendStatus](../Q/QueueBackendStatus.md)
  - FLEXIBLE_ARRAY_MEMBER
- Called from (representative examples):
  - [AsyncShmemSize](AsyncShmemSize.md)
  - [AsyncShmemInit](AsyncShmemInit.md)

## Notes and Other Information
- Protected by NotifyQueueLock and NotifyQueueTailLock with specific locking protocols
- SHARED lock allows backends to inspect their own entries and head/tail pointers
- EXCLUSIVE lock required to inspect other backends' entries or modify head pointer
- Both locks in EXCLUSIVE mode required to modify tail pointers
- Lock ordering to prevent deadlocks: NotifyQueueTailLock, then NotifyQueueLock, then SLRU bank lock
- [Backend](../B/Backend.md) array indexed by ProcNumber for efficient SendProcSignal operations
- Active listeners are threaded together in ProcNumber order for cache-friendly scanning
- Part of the SLRU (Simple Least Recently Used) buffer management system for persistent storage