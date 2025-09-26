# HotStandbyState

## Location
[src/include/access/xlogutils.h:53-56](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xlogutils.h#L53-L56)

## Overview
An enumeration that tracks the current state of Hot Standby functionality during WAL recovery, controlling when read-only queries can be accepted on a standby server.

## Definition

```c
typedef enum
{
	BLK_NEEDS_REDO,				/* changes from WAL record need to be applied */
	BLK_DONE,					/* block is already up-to-date */
	BLK_RESTORED,				/* block was restored from a full-page image */
	BLK_NOTFOUND,				/* block was not found (and hence does not
								 * need to be replayed) */
} XLogRedoAction;
```
## Detailed Description
HotStandbyState is a critical enumeration that manages the progression of Hot Standby functionality during PostgreSQL recovery. It ensures that read-only connections are only allowed when the system has sufficient transaction visibility information to provide consistent snapshots.

The state is only meaningful in the startup process - in all other processes, standbyState will have the value STANDBY_DISABLED, making InHotStandby read as false. This design ensures centralized control of Hot Standby state transitions during recovery.

The enum progresses through states as the startup process initializes transaction tracking and accumulates enough information about primary server transactions to safely serve read-only queries. Each state transition represents a milestone in the recovery process where more functionality becomes available.

The companion macro  is defined as  and is used throughout the codebase to determine if Hot Standby mode is actively serving queries.

## Parameters / Member Variables
- : Hot Standby is disabled, either due to crash recovery mode or postgresql.conf configuration
- : Recovery transaction environment has been initialized, but transaction tracking is not yet ready
- : Transaction tracking is initialized but information may be incomplete, connections not yet allowed
- : Complete transaction knowledge available, read-only queries and snapshots are permitted

## Dependencies
- Functions called/Symbols referenced:
  - Used with InHotStandby macro (src/include/access/xlogutils.h:57)
  - standbyState global variable (src/backend/access/transam/xlogutils.c:53)
- Referenced extensively by:
  -  (standby.c:143 - sets STANDBY_INITIALIZED)
  -  (procarray.c:1263, 1270 - manages SNAPSHOT_PENDING/READY transitions)
  -  (xlog.c:6071, 6164 - checks state for checkpoint behavior)
  -  (xact.c:6095, 6235 - transaction commit handling)
  -  (procarray.c:1025, 1061 - transaction data collection)

## Notes and Other Information
- State transitions are unidirectional during a recovery session - once advanced, the state does not regress
- Only the startup process modifies this state; other processes always see STANDBY_DISABLED
- The state progression mirrors the accumulation of transaction visibility information needed for consistent read operations
- Critical for preventing read-only queries before sufficient transaction tracking data is available
- State transitions are triggered by processing specific WAL records (RUNNING_XACTS, shutdown checkpoints)
- Used extensively in assertions throughout the codebase to ensure proper state management during recovery
- The SNAPSHOT_PENDING state allows redo functions to update in-memory state while still preventing user connections