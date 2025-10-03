# XLogInitNewTimeline

## Location
[src/backend/access/transam/xlog.c:5169-5243](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L5169-L5243)

## Overview
Initializes the first WAL segment on a new timeline after recovery, handling the transition from an old timeline to a new one during archive recovery completion.

## Definition

```c
static void
XLogInitNewTimeline(TimeLineID endTLI, XLogRecPtr endOfLog, TimeLineID newTLI)
```
## Detailed Description
XLogInitNewTimeline manages the critical transition from one timeline to another during PostgreSQL recovery operations. This function is called when the system needs to create a new timeline, typically after completing archive recovery and before normal operations resume.

The function handles two distinct scenarios:
1. **Mid-segment timeline switch**: When the timeline change occurs in the middle of a WAL segment, it copies the partial data from the old timeline's segment to create the corresponding segment on the new timeline
2. **Segment boundary switch**: When the timeline change occurs exactly at a segment boundary, it simply creates a new segment on the new timeline

The function ensures proper WAL continuity across timeline transitions by:
- Updating the minimum recovery point one final time
- Calculating the correct segment numbers for both old and new timelines
- Creating the appropriate WAL segments with proper data preservation
- Cleaning up any archival status files for the new segment

This timeline initialization is crucial for maintaining WAL integrity and ensuring that the database can properly continue operations on the new timeline.

## Parameters / Member Variables
- `endTLI`: Timeline ID of the old/ending timeline
- `endOfLog`: WAL record pointer indicating the end position of the old timeline
- `newTLI`: Timeline ID of the new timeline being initialized
## Dependencies
- Functions called/Symbols referenced:
  - [UpdateMinRecoveryPoint](../U/UpdateMinRecoveryPoint.md): Updates minimum recovery point tracking
  - XLByteToPrevSeg: Calculates the previous segment number from WAL position
  - XLByteToSeg: Calculates segment number from WAL position
  - [XLogFileCopy](XLogFileCopy.md): Copies WAL data between segments on different timelines
  - XLogSegmentOffset: Calculates offset within a WAL segment
  - [XLogFileInit](XLogFileInit.md): Creates and initializes a new WAL segment file
  - [XLogFileName](XLogFileName.md): Generates WAL filename for given timeline and segment
  - [XLogArchiveCleanup](XLogArchiveCleanup.md): Removes archive status files for a WAL segment

- Called from (representative examples):
  - [StartupXLOG](../S/StartupXLOG.md): Called during recovery completion to establish new timeline

## Notes and Other Information
- This is a static function internal to the xlog.c module
- Always called after archive recovery when switching to a new timeline
- Includes assertion that the old and new timeline IDs must be different
- Handles both partial segment copying and new segment creation scenarios
- Ensures no stale archive notification files exist for the new timeline segment
- Critical for maintaining WAL continuity across timeline transitions
- The function is designed to work before normal WAL writing is allowed, so locking considerations are minimal

## Simplified Source

```c
// Simplified version of XLogInitNewTimeline
static void
XLogInitNewTimeline(TimeLineID endTLI, XLogRecPtr endOfLog, TimeLineID newTLI)
{
    char xlogfname[MAXFNAMELEN];
    XLogSegNo endLogSegNo;
    XLogSegNo startLogSegNo;

    // Ensure we're actually switching to a different timeline
    Assert(endTLI != newTLI);

    // Update recovery tracking one final time
    UpdateMinRecoveryPoint(InvalidXLogRecPtr, true);

    // Calculate segment numbers for old and new timelines
    XLByteToPrevSeg(endOfLog, endLogSegNo, wal_segment_size);
    XLByteToSeg(endOfLog, startLogSegNo, wal_segment_size);

    // Handle timeline switch based on whether it occurs mid-segment or at boundary
    if (endLogSegNo == startLogSegNo) {
        // Mid-segment switch: copy partial data from old timeline
        XLogFileCopy(newTLI, endLogSegNo, endTLI, endLogSegNo,
                     XLogSegmentOffset(endOfLog, wal_segment_size));
    } else {
        // Segment boundary switch: create new segment on new timeline
        int fd = XLogFileInit(startLogSegNo, newTLI);

        if (close(fd) != 0) {
            XLogFileName(xlogfname, newTLI, startLogSegNo, wal_segment_size);
            ereport(ERROR, (errcode_for_file_access(),
                           errmsg("could not close file \"%s\": %m", xlogfname)));
        }
    }

    // Clean up any stale archive status files for the new segment
    XLogFileName(xlogfname, newTLI, startLogSegNo, wal_segment_size);
    XLogArchiveCleanup(xlogfname);
}
```

Key simplifications made:
- Consolidated variable declarations for clarity
- Added descriptive comments for each major logic section
- Preserved essential error handling for file operations
- Maintained the core branching logic for mid-segment vs boundary switches
- Simplified complex comments into concise explanations
- Kept all critical function calls and assertions