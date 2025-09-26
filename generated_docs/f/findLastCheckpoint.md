# findLastCheckpoint

## Location
[src/bin/pg_rewind/parsexlog.c:168-274](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/parsexlog.c#L168-L274)

## Overview
Searches backwards through WAL records from a given fork point to find the last checkpoint record that occurred before the fork, essential for pg_rewind to determine the safe starting point for synchronization.

## Definition

```c
void
findLastCheckpoint(const char *datadir, XLogRecPtr forkptr, int tliIndex,
				   XLogRecPtr *lastchkptrec, TimeLineID *lastchkpttli,
				   XLogRecPtr *lastchkptredo, const char *restoreCommand)
```
## Detailed Description
This function implements a critical part of pg_rewind's WAL analysis by walking backwards through WAL records to locate the most recent checkpoint that occurred before the WAL fork point. The checkpoint found serves as the safe starting point for data synchronization between the source and target PostgreSQL instances.

The function handles WAL page boundaries correctly, skipping page headers when the fork pointer falls exactly at a page boundary. It reads WAL records backwards by following the xl_prev chain and identifies checkpoint records by examining their resource manager ID and record type. When a valid checkpoint is found (either XLOG_CHECKPOINT_SHUTDOWN or XLOG_CHECKPOINT_ONLINE), it extracts the checkpoint data and returns the relevant information through output parameters.

Additionally, the function tracks WAL filenames that should be preserved during the rewind process by calling keepwal_add_entry() for each WAL segment encountered.

## Parameters / Member Variables
- : Path to the PostgreSQL data directory containing pg_wal subdirectory
- : XLogRecPtr indicating the WAL position where the fork occurred
- : Index into the target timeline history array indicating which timeline to search
- : Output parameter - XLogRecPtr of the found checkpoint record
- : Output parameter - TimeLineID of the found checkpoint 
- : Output parameter - XLogRecPtr of the checkpoint's redo point
- : Command string used to restore archived WAL files if needed (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [XLogReaderAllocate](../X/XLogReaderAllocate.md)
  - [SimpleXLogPageRead](../S/SimpleXLogPageRead.md)
  - [XLogBeginRead](../X/XLogBeginRead.md)
  - [XLogReadRecord](../X/XLogReadRecord.md)
  - XLogSegmentOffset
  - [XLogFileName](../X/XLogFileName.md)
  - XLogRecGetInfo
  - XLogRecGetRmid
  - XLogRecGetData
  - [keepwal_add_entry](../k/keepwal_add_entry.md)
  - [XLogReaderFree](../X/XLogReaderFree.md)
  - [XLogRecord](../X/XLogRecord.md)
  - [CheckPoint](../C/CheckPoint.md)
- Called from (representative examples):
  - [main](../m/main.md) (in src/bin/pg_rewind/pg_rewind.c:461)

## Notes and Other Information
- The search stops at the first checkpoint found before the fork point, not the most recent overall checkpoint
- Properly handles page boundary conditions where fork pointer aligns with page headers
- Tracks WAL files that must be preserved during the rewind operation
- Uses backward chaining through xl_prev pointers to traverse WAL history efficiently
- Critical for ensuring pg_rewind starts from a consistent checkpoint state
- The checkpoint found determines the scope of data that needs to be synchronized