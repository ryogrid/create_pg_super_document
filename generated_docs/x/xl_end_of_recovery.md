# xl_end_of_recovery

## Location
[src/include/access/xlog_internal.h:300-306](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xlog_internal.h#L300-L306)

## Overview
A data structure that marks the end of recovery process in PostgreSQL, logging timeline information and WAL level when recovery completes without performing an END_OF_RECOVERY checkpoint.

## Definition


## Detailed Description
xl_end_of_recovery is a WAL record structure used to mark the completion of the recovery process in PostgreSQL. This record is written when recovery ends but an END_OF_RECOVERY checkpoint is not performed. It serves as a marker in the WAL stream indicating the transition from recovery mode to normal operations.

The structure captures critical information about the timeline transition that occurs at the end of recovery, including the new timeline ID that the server will use going forward and the previous timeline it was recovering from. This information is essential for maintaining proper timeline history and ensuring correct behavior in replication scenarios.

The record is written with the XLOG_END_OF_RECOVERY record type (0x90) and helps coordinate the end of recovery across different components of the PostgreSQL system.

## Parameters / Member Variables
- : Timestamp (with timezone) indicating when recovery completed
- : The new timeline ID that will be used after recovery completes
- : The timeline ID that was being recovered from before the fork
- : The WAL logging level that will be in effect after recovery

## Dependencies
- Functions called/Symbols referenced:
  - TimestampTz (timestamp with timezone type)
  - TimeLineID (timeline identifier type)
- Called from (representative examples):
  - [CreateEndOfRecoveryRecord](../C/CreateEndOfRecoveryRecord.md) (creates and logs end of recovery records)
  - [xlog_desc](xlog_desc.md) (describes end of recovery records for debugging)
  - [xlog_redo](xlog_redo.md) (processes end of recovery records during replay)
  - [ApplyWalRecord](../A/ApplyWalRecord.md) (applies end of recovery records during recovery)
  - [SummarizeXlogRecord](../S/SummarizeXlogRecord.md) (summarizes end of recovery records in WAL)

## Notes and Other Information
- Associated with WAL record type XLOG_END_OF_RECOVERY (0x90)
- Used when recovery ends without performing an END_OF_RECOVERY checkpoint
- Critical for timeline management in PostgreSQL replication
- Helps coordinate the transition from recovery to normal operations
- Timeline information is essential for proper functioning of standby servers
- Part of PostgreSQL's recovery and replication infrastructure
- The record type distinguishes this from other recovery-related records
- Used in both crash recovery and standby promotion scenarios