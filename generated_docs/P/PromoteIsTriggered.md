# PromoteIsTriggered

## Location
[src/backend/access/transam/xlogrecovery.c:4395-4412](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L4395-L4412)

## Overview
Checks whether a standby promotion has been triggered, providing a thread-safe way for any process connected to shared memory to query promotion status.

## Definition
```c
bool PromoteIsTriggered(void)
```

## Detailed Description
This function provides a safe mechanism for any PostgreSQL process connected to shared memory to check if a standby server promotion has been triggered. Unlike `CheckForStandbyTrigger()`, which may have more restricted usage, this function can be called from any process context.

The function implements an optimization strategy where it caches the result locally once a promotion is detected. Since promotions can only happen once per standby lifecycle, there's no need to repeatedly check shared memory after the promotion flag has been set to true.

The function uses spinlock protection when accessing shared memory to ensure thread-safe reads of the `SharedPromoteIsTriggered` flag from `XLogRecoveryCtl`. The result is cached in the local variable `LocalPromoteIsTriggered` to avoid repeated spinlock acquisition.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire (implicitly through spinlock macros)
  - SpinLockRelease (implicitly through spinlock macros)
  - XLogRecoveryCtl (shared memory structure)
  - LocalPromoteIsTriggered (local static variable)
- Called from (representative examples):
  - [PerformRecoveryXLogAction](PerformRecoveryXLogAction.md)
  - [pg_wal_replay_pause](../p/pg_wal_replay_pause.md)
  - [pg_wal_replay_resume](../p/pg_wal_replay_resume.md)
  - [EndOfWalRecoveryInfo](../E/EndOfWalRecoveryInfo.md) (header reference)

## Notes and Other Information
- This is a public function (not static), accessible from other modules
- Thread-safe through spinlock protection when accessing shared memory
- Implements caching optimization to avoid repeated shared memory access after promotion is detected
- Part of PostgreSQL's standby promotion infrastructure
- More broadly accessible than `CheckForStandbyTrigger()` which may have usage restrictions
- Returns true once a promotion is triggered and remains true for the lifetime of the process
- The promotion state is persistent and cannot be "un-triggered" once set