# set_spins_per_delay

## Location
[src/backend/storage/lmgr/s_lock.c:213-223](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/s_lock.c#L213-L223)

## Overview
Sets the local copy of spins_per_delay during backend startup to optimize spinlock performance.

## Definition

```c
struct test_lock_struct
{
	char		pad1;
	slock_t		lock;
	char		pad2;
};
```
## Detailed Description
The  function is a simple but critical function that updates the local thread-specific copy of the  variable during backend initialization. This variable controls how many CPU cycles a process will spin before yielding when waiting for a spinlock, which is a key performance parameter for PostgreSQL's low-level synchronization mechanisms.

The function is designed to be extremely fast since it may be called while holding a spinlock, where any delay could impact system-wide performance. It performs a simple assignment operation to synchronize the local spinlock behavior with the shared system configuration.

## Parameters / Member Variables
- : The shared system-wide value for spins per delay that should be copied to the local thread context

## Dependencies
- Functions called/Symbols referenced:
  - (None - simple assignment operation)
- Called from (representative examples):
  - [InitProcess](../I/InitProcess.md) (src/backend/storage/lmgr/proc.c:334)
  - [InitAuxiliaryProcess](../I/InitAuxiliaryProcess.md) (src/backend/storage/lmgr/proc.c:552)
- Related symbols:
  - DEFAULT_SPINS_PER_DELAY (src/include/storage/s_lock.h:814)

## Notes and Other Information
- This function must be extremely fast as it can be called while holding spinlocks
- The spins_per_delay parameter is crucial for PostgreSQL's spinlock performance tuning
- Different backends may have different optimal spin counts based on their workload characteristics
- The function is called during both regular backend and auxiliary process initialization