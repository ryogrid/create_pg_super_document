# WalSndSegmentOpen

## Location
[src/backend/replication/walsender.c:3022-3099](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walsender.c#L3022-L3099)

## Overview
WalSndSegmentOpen is a callback function used by XLogReader to open WAL segments during WAL streaming replication, handling timeline switches and file access errors appropriately.

## Definition

```c
static void
WalSndSegmentOpen(XLogReaderState *state, XLogSegNo nextSegNo,
				  TimeLineID *tli_p)
```
## Detailed Description
WalSndSegmentOpen serves as the segment_open callback for XLogReaderState during WAL streaming operations. Its primary responsibility is to open the appropriate WAL segment file for reading, taking into account timeline switches that may have occurred. 

The function implements sophisticated logic to handle historic timelines correctly. When streaming from a historic timeline and encountering a timeline switch within a segment, it automatically selects the WAL file from the newer timeline. This is necessary because archive recovery prefers files from newer timelines, and the old timeline's segment file might not exist on disk. The contents are identical up to the switchpoint since PostgreSQL copies the used portion of the old segment to the new timeline's file during timeline switches.

The function also provides clear error handling for missing WAL segments, distinguishing between files that have been removed/recycled versus other file access errors.

## Parameters / Member Variables
- : XLogReaderState structure containing the context for WAL reading operations
- : The WAL segment number to be opened for reading
- : Pointer to TimeLineID that will be set to the appropriate timeline for the segment

## Dependencies
- Functions called/Symbols referenced:
  - XLByteToSeg
  - [XLogFilePath](../X/XLogFilePath.md)
  - [BasicOpenFile](../B/BasicOpenFile.md)
  - [XLogFileName](../X/XLogFileName.md)
  - ereport
- Called from (representative examples):
  - [StartReplication](../S/StartReplication.md)
  - [CreateReplicationSlot](../C/CreateReplicationSlot.md)
  - [StartLogicalReplication](../S/StartLogicalReplication.md)

## Notes and Other Information
- This function is specifically designed as a callback for XLogReaderRoutine->segment_open
- Handles the complex scenario of timeline switches within WAL segments during historic timeline streaming
- Provides detailed error messages when WAL segments are missing, helping diagnose replication issues
- Uses global variables sendTimeLine, sendTimeLineIsHistoric, sendTimeLineValidUpto, and sendTimeLineNextTLI for timeline management
- File access is performed using BasicOpenFile with O_RDONLY | PG_BINARY flags for cross-platform compatibility