# ResolveRecoveryConflictWithBufferPin

## Location
[src/backend/storage/ipc/standby.c:792-875](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/standby.c#L792-L875)

## Overview
Resolves recovery conflicts with backends holding buffer pins by either sending signals immediately when time limits are exceeded or waiting with deadlock detection capabilities.

## Definition

```c
void
ResolveRecoveryConflictWithBufferPin(void)
```
## Detailed Description
This function is called from LockBufferForCleanup() to resolve conflicts with other backends holding buffer pins during hot standby recovery. It implements conflict resolution for buffer pin conflicts, which occur when the startup process needs to clean up a buffer but other backends have it pinned.

The function handles two main scenarios:
1. **Immediate resolution**: When the current time has already exceeded the standby limit time, it immediately sends a signal to all backends to check if they hold conflicting buffer pins
2. **Timed waiting**: When there's still time before the limit, it sets up timeouts (standby timeout and deadlock timeout) and waits for the buffer to be unpinned

The function includes deadlock detection logic because deadlocks can occur when queries wait on locks that can only be cleared by the startup process, but the startup process is also waiting for buffer pins. It protects against deadlocks where the query waits first and then the startup process sleeps.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - GetStandbyLimitTime
  - GetCurrentTimestamp
  - SendRecoveryConflictWithBufferPin
  - ProcWaitForSignal  
  - enable_timeouts
  - disable_all_timeouts
- Called from (representative examples):
  - LockBufferForCleanup (src/backend/storage/buffer/bufmgr.c:5316)

## Notes and Other Information
- Only operates when InHotStandby is true
- Uses two types of timeouts: STANDBY_TIMEOUT and STANDBY_DEADLOCK_TIMEOUT
- Waits specifically for UnpinBuffer() signals or timeout interruptions
- Sends different signal types based on timeout reason (PROCSIG_RECOVERY_CONFLICT_BUFFERPIN vs PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK)
- Has a known issue (marked with XXX) where repeated deadlock check requests may be sent every deadlock_timeout period until resolution
- Assumes only UnpinBuffer() and established timeouts can wake up the waiting process
- Clears all timeouts on exit to avoid interference with other timeout mechanisms
- Deadlock checks are expensive so they're only performed after deadlock_timeout expires