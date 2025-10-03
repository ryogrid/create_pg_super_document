# ReadRecord

## Location
[src/backend/access/transam/xlogrecovery.c:3131-3297](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L3131-L3297)

## Overview
ReadRecord is the core function responsible for reading the next XLOG record during PostgreSQL's WAL recovery process, handling various recovery scenarios including crash recovery, archive recovery, and standby mode.

## Definition

```c
static XLogRecord *
ReadRecord(XLogPrefetcher *xlogprefetcher, int emode,
		   bool fetching_ckpt, TimeLineID replayTLI)
```
## Detailed Description
ReadRecord serves as the primary interface for reading WAL records during recovery operations. It wraps the XLogPrefetcher functionality and provides robust error handling, timeline validation, and recovery mode transitions. The function operates in an infinite loop, attempting to read valid records and handling various failure scenarios including corrupt records, timeline mismatches, and source exhaustion.

Key behaviors include:
- Validates timeline consistency using expectedTLEs
- Handles transitions between crash recovery and archive recovery modes  
- Manages retry logic for standby mode operations
- Tracks incomplete/aborted records for later cleanup
- Provides comprehensive error reporting with appropriate severity levels

## Parameters / Member Variables
- `*xlogprefetcher`: XLogPrefetcher instance that provides the underlying record reading capability
- `emode`: Error reporting mode (PANIC or LOG) that determines how failures are handled
- `fetching_ckpt`: Boolean flag indicating whether this call is fetching a checkpoint record
- `replayTLI`: Timeline ID being replayed, used for timeline validation and recovery transitions
## Dependencies
- Functions called/Symbols referenced:
  - [XLogPrefetcherGetReader](../X/XLogPrefetcherGetReader.md)
  - [XLogPrefetcherReadRecord](../X/XLogPrefetcherReadRecord.md)
  - [emode_for_corrupt_record](../e/emode_for_corrupt_record.md)
  - [tliInHistory](../t/tliInHistory.md)
  - [SwitchIntoArchiveRecovery](../S/SwitchIntoArchiveRecovery.md)
  - [CheckRecoveryConsistency](../C/CheckRecoveryConsistency.md)
  - [EnableStandbyMode](../E/EnableStandbyMode.md)
  - [CheckForStandbyTrigger](../C/CheckForStandbyTrigger.md)
- Called from (representative examples):
  - [InitWalRecovery](../I/InitWalRecovery.md)
  - [PerformWalRecovery](../P/PerformWalRecovery.md)
  - [FinishWalRecovery](../F/FinishWalRecovery.md)
  - [ReadCheckpointRecord](ReadCheckpointRecord.md)

## Notes and Other Information
- The function maintains global state variables like lastSourceFailed and currentSource to coordinate retry logic
- Timeline validation prevents replay of records from unexpected timeline branches
- The transition from crash recovery to archive recovery is handled transparently when ArchiveRecoveryRequested is true
- In standby mode, the function will retry indefinitely until a valid record is found or a standby trigger occurs
- Incomplete records are tracked via abortedRecPtr and missingContrecPtr for later overwrite record generation

## Simplified Source

```c
// Simplified version of ReadRecord
static XLogRecord *
ReadRecord(XLogPrefetcher *xlogprefetcher, int emode,
           bool fetching_ckpt, TimeLineID replayTLI)
{
    XLogRecord *record;
    XLogReaderState *xlogreader = XLogPrefetcherGetReader(xlogprefetcher);
    XLogPageReadPrivate *private = (XLogPageReadPrivate *) xlogreader->private_data;

    // Setup parameters for XLogPageRead
    private->fetching_ckpt = fetching_ckpt;
    private->emode = emode;
    private->randAccess = (xlogreader->ReadRecPtr == InvalidXLogRecPtr);
    private->replayTLI = replayTLI;
    lastSourceFailed = false;

    for (;;)
    {
        char *errormsg;

        // Core logic step 1: Attempt to read next WAL record
        record = XLogPrefetcherReadRecord(xlogprefetcher, &errormsg);

        if (record == NULL)
        {
            // Core logic step 2: Handle incomplete records for later cleanup
            if (!ArchiveRecoveryRequested &&
                !XLogRecPtrIsInvalid(xlogreader->abortedRecPtr))
            {
                abortedRecPtr = xlogreader->abortedRecPtr;
                missingContrecPtr = xlogreader->missingContrecPtr;
            }

            // Close file and report error if present
            if (readFile >= 0) {
                close(readFile);
                readFile = -1;
            }
            if (errormsg)
                ereport(emode_for_corrupt_record(emode, xlogreader->EndRecPtr),
                        (errmsg_internal("%s", errormsg)));
        }
        // Core logic step 3: Validate timeline consistency
        else if (!tliInHistory(xlogreader->latestPageTLI, expectedTLEs))
        {
            // Report timeline mismatch error
            ereport(emode_for_corrupt_record(emode, xlogreader->EndRecPtr),
                    (errmsg("unexpected timeline ID %u in WAL segment...",
                            xlogreader->latestPageTLI)));
            record = NULL;
        }

        // Core logic step 4: Return valid record or handle recovery transitions
        if (record)
        {
            return record;  // Success - got a valid record
        }
        else
        {
            lastSourceFailed = true;

            // Core logic step 5: Transition from crash to archive recovery if needed
            if (!InArchiveRecovery && ArchiveRecoveryRequested && !fetching_ckpt)
            {
                InArchiveRecovery = true;
                if (StandbyModeRequested)
                    EnableStandbyMode();

                SwitchIntoArchiveRecovery(xlogreader->EndRecPtr, replayTLI);
                minRecoveryPoint = xlogreader->EndRecPtr;
                minRecoveryPointTLI = replayTLI;
                CheckRecoveryConsistency();

                // Reset state for archive retry
                lastSourceFailed = false;
                currentSource = XLOG_FROM_ANY;
                continue;
            }

            // Core logic step 6: Retry in standby mode or give up
            if (StandbyMode && !CheckForStandbyTrigger())
                continue;  // Retry indefinitely in standby mode
            else
                return NULL;  // Give up in non-standby mode
        }
    }
}
```

Key simplifications made:
- Removed detailed error handling and file name generation for timeline mismatches
- Consolidated complex conditional logic into clearer flow
- Abstracted low-level memory and file operations with comments
- Simplified variable declarations and eliminated temporary variables
- Removed detailed comments explaining recovery scenarios, keeping only essential logic markers
- Condensed similar error reporting patterns into representative examples