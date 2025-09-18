# ReadRecord

## Location
src/backend/access/transam/xlogrecovery.c: 3131 - 3297

## Overview
ReadRecord is the core function responsible for reading the next XLOG record during PostgreSQL's WAL recovery process, handling various recovery scenarios including crash recovery, archive recovery, and standby mode.

## Definition


## Detailed Description
ReadRecord serves as the primary interface for reading WAL records during recovery operations. It wraps the XLogPrefetcher functionality and provides robust error handling, timeline validation, and recovery mode transitions. The function operates in an infinite loop, attempting to read valid records and handling various failure scenarios including corrupt records, timeline mismatches, and source exhaustion.

Key behaviors include:
- Validates timeline consistency using expectedTLEs
- Handles transitions between crash recovery and archive recovery modes  
- Manages retry logic for standby mode operations
- Tracks incomplete/aborted records for later cleanup
- Provides comprehensive error reporting with appropriate severity levels

## Parameters / Member Variables
- : XLogPrefetcher instance that provides the underlying record reading capability
- : Error reporting mode (PANIC or LOG) that determines how failures are handled
- : Boolean flag indicating whether this call is fetching a checkpoint record
- : Timeline ID being replayed, used for timeline validation and recovery transitions

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