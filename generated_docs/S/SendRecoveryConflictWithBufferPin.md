# SendRecoveryConflictWithBufferPin

## Location
[src/backend/storage/ipc/standby.c:876-903](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/standby.c#L876-L903)

## Overview
Sends signals to all database backends to ask them to check if they are holding buffer pins that are delaying the startup process during recovery conflicts.

## Definition

```c
static void
SendRecoveryConflictWithBufferPin(ProcSignalReason reason)
```
## Detailed Description
This static function serves as a helper for buffer pin conflict resolution during hot standby recovery. It broadcasts signals to all backends in the system to request them to check whether they are holding buffer pins that might be blocking the startup process from proceeding.

The function is designed with a non-destructive approach - it doesn't immediately terminate or force backends to release pins. Instead, it sends a signal that allows each backend to examine its own state and decide the appropriate action. This prevents innocent backends (those not actually holding conflicting pins) from being unnecessarily affected.

The function accepts two types of signal reasons:
1. **PROCSIG_RECOVERY_CONFLICT_BUFFERPIN**: Used when buffer pin conflicts need to be resolved
2. **PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK**: Used when potential startup deadlocks involving buffer pins need to be checked

## Parameters / Member Variables
- : The type of signal to send to backends, must be either PROCSIG_RECOVERY_CONFLICT_BUFFERPIN or PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK

## Dependencies
- Functions called/Symbols referenced:
  - CancelDBBackends (with InvalidOid, reason, false parameters)
- Called from (representative examples):
  - ResolveRecoveryConflictWithBufferPin (multiple calls at lines 805, 845, 861)

## Notes and Other Information
- This is a static function, only accessible within the same source file
- Uses InvalidOid as the database OID parameter to CancelDBBackends, meaning it targets all databases
- The third parameter (false) to CancelDBBackends indicates that the conflict flag should not be set immediately
- Employs a 'ask first, decide later' strategy where backends self-determine their fate upon receiving the signal
- Part of the broader recovery conflict resolution mechanism in PostgreSQL hot standby
- The function includes an assertion to validate that only appropriate signal reasons are used