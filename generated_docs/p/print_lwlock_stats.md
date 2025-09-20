# print_lwlock_stats

## Location
[src/backend/storage/lmgr/lwlock.c:347-370](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lwlock.c#L347-L370)

## Overview
Outputs collected lightweight lock statistics to stderr, providing detailed usage information for each tracked lock.

## Definition

```c
static void
print_lwlock_stats(int code, Datum arg)
```
## Detailed Description
This function serves as an exit handler that prints accumulated lightweight lock statistics when a PostgreSQL process terminates. It iterates through the lwlock_stats hash table and outputs detailed statistics for each lock that was tracked during the process lifetime. The output includes acquisition counts (shared and exclusive), blocking events, spin delays, and self-dequeue operations.

The function acquires an exclusive lock on the first element of MainLWLockArray to prevent multiple backends from mixing their statistical reports, ensuring clean, readable output even in multi-process environments.

## Parameters / Member Variables
- : Exit code parameter (unused in function body)
- : Datum argument passed from exit handler registration (unused in function body)

## Dependencies
- Functions called/Symbols referenced:
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
  - LWLockAcquire
  - LWLockRelease
  - GetLWTrancheName
  - fprintf
- Types referenced:
  - HASH_SEQ_STATUS
  - lwlock_stats
- Global variables accessed:
  - lwlock_stats_htab
  - MainLWLockArray
  - MyProcPid
- Called from:
  - [init_lwlock_stats](../i/init_lwlock_stats.md) (registered as exit handler via on_shmem_exit)
  - LOG_LWDEBUG (src/backend/storage/lmgr/lwlock.c:308)

## Notes and Other Information
- Registered as a shared memory exit handler by init_lwlock_stats
- Uses the first LWLock in MainLWLockArray as a mutex to serialize output from multiple backends
- Output format includes process ID, lock tranche name, instance pointer, and various counters
- Statistics include: shared acquisitions, exclusive acquisitions, blocks, spin delays, and self-dequeue counts
- Only compiled and functional when LWLOCK_STATS debugging is enabled
- Output goes to stderr to separate it from normal application output