# WalSndKill

## Location
[src/backend/replication/walsender.c:3004-3021](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walsender.c#L3004-L3021)

## Overview
Cleanup function that destroys the per-walsender data structure when a WAL sender process terminates.

## Definition
```c
static void WalSndKill(int code, Datum arg)
```

## Detailed Description
This function serves as an exit handler that properly cleans up the WAL sender slot in shared memory when a WAL sender process terminates. It ensures thread-safe cleanup by clearing the latch pointer and marking the slot as available by resetting the PID field to zero. The function is designed to be called automatically during process exit through the shared memory exit callback mechanism.

The cleanup process involves careful ordering of operations under spinlock protection to prevent race conditions with other processes that might be scanning for available WAL sender slots.

## Parameters / Member Variables
- `code`: Exit code passed by the exit handler mechanism (unused)
- `arg`: Additional argument passed by the exit handler mechanism (unused)

## Dependencies
- Functions called/Symbols referenced:
  - [WalSnd](WalSnd.md) (structure type)
  - SpinLockAcquire/SpinLockRelease
- Called from:
  - [InitWalSenderSlot](../I/InitWalSenderSlot.md) (src/backend/replication/walsender.c:2999) - registered as exit handler

## Notes and Other Information
- Registered as exit handler via on_shmem_exit() in InitWalSenderSlot()
- Follows standard PostgreSQL exit handler signature (int code, Datum arg)
- Clears latch pointer while holding spinlock to ensure safe concurrent reads
- Marks slot as available by setting pid to 0, making it discoverable by new WAL sender processes
- Critical for preventing resource leaks and ensuring proper slot recycling
- Thread-safe cleanup ensures other processes can reliably detect slot availability
- Asserts that MyWalSnd is valid before cleanup to catch programming errors
- Sets MyWalSnd to NULL after cleanup to prevent accidental reuse