# EndPrepare

## Location
[src/backend/access/transam/twophase.c:1142-1263](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L1142-L1263)

## Overview
EndPrepare completes the two-phase commit state file preparation by finalizing the state data and writing it to the Write-Ahead Log (WAL).

## Definition

```c
void
EndPrepare(GlobalTransaction gxact)
```
## Detailed Description
EndPrepare finalizes the two-phase commit preparation process initiated by StartPrepare. It adds an end sentinel record to the 2PC records, updates the total length in the file header, handles replication origin information if present, validates the data size doesn't exceed limits, and writes the entire state data to WAL within a critical section. The function ensures proper checkpoint coordination, marks the transaction as prepared, and handles synchronous replication requirements.

## Parameters / Member Variables
- : GlobalTransaction structure representing the transaction being prepared, which will store prepare LSN information and be marked as prepared

## Dependencies
- Functions called/Symbols referenced:
  - [RegisterTwoPhaseRecord](../R/RegisterTwoPhaseRecord.md)
  - [XLogEnsureRecordSpace](../X/XLogEnsureRecordSpace.md)
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - [XLogSetRecordFlags](../X/XLogSetRecordFlags.md)
  - [XLogInsert](../X/XLogInsert.md)
  - [XLogFlush](../X/XLogFlush.md)
  - [MarkAsPrepared](../M/MarkAsPrepared.md)
  - [SyncRepWaitForLSN](../S/SyncRepWaitForLSN.md)
  - [replorigin_session_advance](../r/replorigin_session_advance.md)
- Called from (representative examples):
  - [PrepareTransaction](../P/PrepareTransaction.md)

## Notes and Other Information
- Uses DELAY_CHKPT_START flags to coordinate with checkpoint process to ensure state file is properly fsync'd
- Validates total data size against MaxAllocSize to prevent issues during recovery
- Handles replication origin metadata (LSN and timestamp) for logical replication scenarios
- Creates WAL record of type XLOG_XACT_PREPARE with XLOG_INCLUDE_ORIGIN flag
- Maintains critical section semantics around WAL writing and transaction state changes
- Sets MyLockedGxact to ensure proper cleanup if process crashes after preparation
- Waits for synchronous replication confirmation if required by configuration
- Cleans up the records data structure after successful completion