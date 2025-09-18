# RecoveryState

## Location
[src/include/access/xlog.h:92-96](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xlog.h#L92-L96)

## Overview
An enumeration that represents the different states of database recovery operations in PostgreSQL's WAL recovery system.

## Definition


## Detailed Description
RecoveryState tracks the current state of PostgreSQL's recovery process, which occurs during database startup after an unclean shutdown or when performing point-in-time recovery. RECOVERY_STATE_CRASH indicates the database is performing crash recovery, replaying WAL records from the last checkpoint to ensure consistency. RECOVERY_STATE_ARCHIVE indicates the database is in archive recovery mode, typically used for point-in-time recovery or standby server initialization. RECOVERY_STATE_DONE indicates that recovery has completed and the database is running in normal production mode, accepting read-write operations.

## Parameters / Member Variables
- : Database is performing crash recovery after unclean shutdown
- : Database is performing archive recovery (PITR or standby initialization)
- : Recovery is complete, database is in normal production mode

## Dependencies
- Functions called/Symbols referenced:
  - None (enum type definition)
- Called from (representative examples):
  - [XLogCtlData](../X/XLogCtlData.md) structure (src/backend/access/transam/xlog.c:517)
  - [RecoveryInProgress](RecoveryInProgress.md) function (src/backend/access/transam/xlog.c:6348)
  - [GetRecoveryState](../G/GetRecoveryState.md) function (src/backend/access/transam/xlog.c:6351)
  - [WALAvailability](../W/WALAvailability.md) structure (src/include/access/xlog.h:223)

## Notes and Other Information
- Used to coordinate different behaviors during various recovery phases
- Critical for determining when the database is safe for read-write operations
- Part of PostgreSQL's crash recovery and point-in-time recovery mechanisms
- The transition between states is managed by the startup process during database initialization
- Applications can query recovery state to determine database readiness