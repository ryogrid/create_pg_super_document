# XLogInitNewTimeline

## Location
src/backend/access/transam/xlog.c: 5169 - 5243

## Overview
Initializes the first WAL segment on a new timeline after recovery, handling the transition from an old timeline to a new one during archive recovery completion.

## Definition


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
- : Timeline ID of the old/ending timeline
- : WAL record pointer indicating the end position of the old timeline
- : Timeline ID of the new timeline being initialized

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