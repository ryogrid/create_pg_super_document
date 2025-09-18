# TruncateMultiXact

## Location
src/backend/access/transam/multixact.c: 3094 - 3268

## Overview
TruncateMultiXact removes obsolete MultiXact offset and member segments, coordinating the cleanup of both data structures while ensuring consistency and proper WAL logging.

## Definition
void TruncateMultiXact(MultiXactId newOldestMulti, Oid newOldestMultiDB)

## Detailed Description
This function is the main entry point for MultiXact truncation operations. It safely removes all MultiXactOffset and MultiXactMember segments that are no longer needed, based on the oldest MultiXact ID still of interest. The function operates exclusively on primary servers as part of vacuum operations (via vac_truncate_clog()).

The truncation process is complex and involves several critical steps:
1. Acquiring exclusive locks to prevent concurrent truncations
2. Scanning the directory structure to find the earliest existing pages
3. Computing safe truncation points for both offsets and members
4. Performing the actual truncation in a critical section with WAL logging
5. Updating in-memory state to reflect the new boundaries

The function includes extensive safety checks to handle edge cases like wraparound detection, missing segments, and recovery scenarios. During recovery, truncation is handled by replaying WAL records rather than calling this function directly.

## Parameters / Member Variables
- `newOldestMulti`: The oldest MultiXact ID that must be preserved (new truncation boundary)
- `newOldestMultiDB`: Database ID that is preventing newOldestMulti from advancing further

## Dependencies
- Functions called/Symbols referenced:
  - SlruScanDirectory
  - SlruScanDirCbFindEarliest
  - find_multixact_start
  - PerformMembersTruncation
  - PerformOffsetsTruncation
  - WriteMTruncateXlogRec
  - RecoveryInProgress
  - MultiXactIdIsValid
  - MultiXactIdPrecedesOrEquals
  - MultiXactIdPrecedes
- Called from (representative examples):
  - vac_truncate_clog

## Notes and Other Information
- Only runs on primary servers, never during recovery
- Uses MultiXactTruncationLock to ensure only one truncation at a time
- Includes critical section with checkpoint delay to ensure WAL consistency
- Handles complex edge cases like segment wraparound and missing data
- Updates both in-memory state and persistent storage atomically
- Extensive DEBUG1 logging for troubleshooting truncation operations
- Gracefully handles cases where truncation cannot proceed safely