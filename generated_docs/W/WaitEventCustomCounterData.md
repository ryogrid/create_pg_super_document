# WaitEventCustomCounterData

## Location
[src/backend/utils/activity/wait_event.c:85-89](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/wait_event.c#L85-L89)

## Overview
A shared memory structure that manages the dynamic allocation counter for custom wait events, ensuring thread-safe generation of unique wait event IDs.

## Definition

```c
typedef struct WaitEventCustomCounterData
{
	int			nextId;			/* next ID to assign */
	slock_t		mutex;			/* protects the counter */
} WaitEventCustomCounterData;
```
## Detailed Description
WaitEventCustomCounterData is a critical component of PostgreSQL's custom wait event system that manages the allocation of unique identifiers for custom wait events. This structure maintains a counter that tracks the next available ID to be assigned to newly registered custom wait events. The structure is protected by a spinlock (mutex) to ensure thread-safe access in PostgreSQL's multi-process environment, preventing race conditions when multiple processes attempt to register custom wait events simultaneously.

The counter starts from a base value and increments for each new custom wait event registration, ensuring that each custom wait event receives a unique identifier that can be used for tracking and reporting purposes.

## Parameters / Member Variables
- : An integer containing the next ID number to be assigned to a new custom wait event
- : A spinlock (slock_t) that protects concurrent access to the counter, ensuring atomic updates

## Dependencies
- Functions called/Symbols referenced:
  - slock_t (PostgreSQL spinlock type for lightweight synchronization)
- Called from (representative examples):
  - WaitEventCustomShmemSize
  - WaitEventCustomShmemInit

## Notes and Other Information
- This structure is allocated in shared memory during PostgreSQL startup to be accessible across all backend processes
- The spinlock provides lightweight synchronization suitable for the brief critical section needed to increment the counter
- Essential for maintaining unique wait event IDs across the entire PostgreSQL instance
- The counter is initialized during shared memory setup and persists for the lifetime of the PostgreSQL server
- Used internally by the wait event registration system to ensure no duplicate IDs are assigned
- Part of the broader wait event infrastructure that enables extensions and custom code to register their own wait events for monitoring