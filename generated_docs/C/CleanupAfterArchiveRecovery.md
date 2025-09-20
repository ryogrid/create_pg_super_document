# CleanupAfterArchiveRecovery

## Location
[src/backend/access/transam/xlog.c:5244-5339](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L5244-L5339)

## Overview
Performs comprehensive cleanup operations at the conclusion of archive recovery, including command execution, timeline cleanup, and partial segment handling.

## Definition

```c
static void
CleanupAfterArchiveRecovery(TimeLineID EndOfLogTLI, XLogRecPtr EndOfLog,
							TimeLineID newTLI)
```
## Detailed Description
CleanupAfterArchiveRecovery orchestrates the final cleanup phase when archive recovery completes and the system transitions to normal operation on a new timeline. This function handles several critical cleanup tasks:

1. **Recovery End Command Execution**: Executes the user-configured recovery_end_command if specified, allowing custom scripts to run at recovery completion.

2. **Timeline Cleanup**: Removes WAL segments from the old timeline that are not part of the new timeline's history, preventing confusion and saving disk space.

3. **Partial Segment Handling**: Manages the complex case where the timeline switch occurs in the middle of a WAL segment. The function implements a sophisticated strategy:
   - Renames the partial segment with a .partial suffix
   - Archives it for potential future debugging or manual recovery
   - Ensures no conflicts with existing archive status files

The partial segment handling addresses a key architectural challenge: maintaining recoverability while preventing archive conflicts. By renaming to .partial, the segment becomes available for manual intervention but won't interfere with normal recovery operations.

Special consideration is given to WAL summarization - if WAL summarization is active, the function waits for summarization to complete before renaming partial files to prevent failures.

## Parameters / Member Variables
- : Timeline ID where recovery ended (the old timeline)
- : WAL record pointer indicating the exact end position of recovery
- : Timeline ID of the new timeline being established

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteRecoveryCommand](../E/ExecuteRecoveryCommand.md): Executes the recovery_end_command script
  - [RemoveNonParentXlogFiles](../R/RemoveNonParentXlogFiles.md): Removes WAL files not part of new timeline history
  - XLogSegmentOffset: Calculates offset within a WAL segment
  - XLogArchivingActive: Checks if WAL archiving is currently enabled
  - XLByteToPrevSeg: Calculates segment number from WAL position
  - [XLogFileName](../X/XLogFileName.md): Generates WAL filename for given timeline and segment
  - [XLogArchiveIsReadyOrDone](../X/XLogArchiveIsReadyOrDone.md): Checks archive status of a WAL segment
  - [WaitForWalSummarization](../W/WaitForWalSummarization.md): Waits for WAL summarization to complete
  - XLogFilePath: Constructs full path to WAL file
  - [XLogArchiveCleanup](../X/XLogArchiveCleanup.md): Removes archive status files
  - [durable_rename](../d/durable_rename.md): Performs atomic file rename with error handling
  - [XLogArchiveNotify](../X/XLogArchiveNotify.md): Creates archive notification for a WAL segment

- Called from (representative examples):
  - [StartupXLOG](../S/StartupXLOG.md): Called during recovery completion phase
  - RefreshXLogWriteResult: Called when updating WAL write results

## Notes and Other Information
- This is a static function internal to the xlog.c module
- Implements complex logic for handling partial segments to balance recoverability with archive integrity
- The .partial suffix prevents automatic recovery from using these segments
- Administrators can manually restore .partial segments for debugging or special recovery scenarios
- Waits for WAL summarization to complete before file operations to prevent conflicts
- Only processes partial segments when archiving is active and the segment is truly partial
- Critical for maintaining system consistency during the archive recovery to normal operation transition