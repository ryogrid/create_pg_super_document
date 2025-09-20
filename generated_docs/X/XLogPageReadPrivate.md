# XLogPageReadPrivate

## Location
[src/bin/pg_rewind/parsexlog.c:47-51](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/parsexlog.c#L47-L51)

## Overview
XLogPageReadPrivate is a private data structure used to pass parameters from higher-level WAL record reading functions down to the XLogPageRead callback function during PostgreSQL's write-ahead logging (WAL) recovery operations.

## Definition

```c
typedef struct XLogPageReadPrivate
{
	const char *restoreCommand;
	int			tliIndex;
} XLogPageReadPrivate;
```
## Detailed Description
This structure serves as a communication mechanism between the high-level WAL recovery logic and the low-level page reading operations. It encapsulates context information that the XLogPageRead callback function needs to make appropriate decisions about error handling, timeline selection, and access patterns during WAL recovery.

The structure is typically allocated and initialized in InitWalRecovery and passed to the XLogReader through its private_data field. This allows the XLogPageRead callback to access recovery-specific parameters without requiring them as direct function parameters.

## Parameters / Member Variables
- `restoreCommand`: String containing the restore command for retrieving WAL files
- `tliIndex`: Index into the timeline array for timeline-specific operations

## Dependencies
- Functions called/Symbols referenced:
  - TimeLineID (type)
- Called from (representative examples):
  - [InitWalRecovery](../I/InitWalRecovery.md) (allocation and initialization at src/backend/access/transam/xlogrecovery.c:515,554)
  - [ReadRecord](../R/ReadRecord.md) (usage at src/backend/access/transam/xlogrecovery.c:3136)
  - [XLogPageRead](XLogPageRead.md) (dereferenced at src/backend/access/transam/xlogrecovery.c:3301,3302)
  - [extractPageMap](../e/extractPageMap.md) (pg_rewind usage at src/bin/pg_rewind/parsexlog.c:72)
  - [readOneRecord](../r/readOneRecord.md) (pg_rewind usage at src/bin/pg_rewind/parsexlog.c:130)
  - [findLastCheckpoint](../f/findLastCheckpoint.md) (pg_rewind usage at src/bin/pg_rewind/parsexlog.c:177)
  - [SimpleXLogPageRead](../S/SimpleXLogPageRead.md) (pg_rewind usage at src/bin/pg_rewind/parsexlog.c:278)

## Notes and Other Information
- This structure is defined in src/backend/access/transam/xlogrecovery.c at lines 194-200
- The structure is used both in the main PostgreSQL backend for WAL recovery and in utility programs like pg_rewind for analyzing WAL records
- The private data pattern allows the XLogReader framework to remain generic while enabling specific recovery contexts to pass custom parameters to their page reading callbacks
- Memory for this structure is typically allocated using palloc0() to ensure all fields are properly initialized to zero/false
- The structure enables different error handling strategies depending on the recovery context (e.g., more permissive during checkpoint fetching)