# SetOffsetVacuumLimit

## Location
src/backend/access/transam/multixact.c: 2705 - 2831

## Overview
Determines how aggressively vacuum needs to run to prevent member wraparound by calculating the oldest member offset and installing limit information in MultiXactState.

## Definition


## Detailed Description
This function determines the oldest MultiXact member offset and installs limit information in MultiXactState to prevent overrun of old data in the members SLRU area. It calculates whether emergency autovacuum is required based on the distance between the next offset and oldest offset. The function handles special cases where no multixacts exist and accounts for bugs in early PostgreSQL 9.3.X and 9.4.X releases where the oldest multixact might not actually exist on disk.

The function acquires locks to prevent concurrent truncation and reads shared memory state. It computes a stop limit by moving back to the start of the corresponding segment and leaving one segment before the wraparound point as a safety buffer.

## Parameters / Member Variables
- : Boolean indicating whether this is being called during startup (affects logging behavior)

## Dependencies
- Functions called/Symbols referenced:
  - find_multixact_start
  - LWLockAcquire
  - LWLockRelease
  - ereport
  - errmsg_internal
  - errmsg
  - MULTIXACT_MEMBERS_PER_PAGE
  - SLRU_PAGES_PER_SEGMENT
  - MULTIXACT_MEMBER_SAFE_THRESHOLD
- Called from (representative examples):
  - debug_elog6 (src/backend/access/transam/multixact.c:413)
  - SetMultiXactIdLimit (src/backend/access/transam/multixact.c:2440)

## Notes and Other Information
- Returns true if emergency autovacuum is required, false otherwise
- Protects against member wraparound in the MultiXact SLRU
- Uses MultiXactTruncationLock and MultiXactGenLock for synchronization
- Handles edge cases where oldest multixact data might be missing due to historical bugs
- Logs diagnostic information about member offsets and wraparound protection status
- Function is located at src/backend/access/transam/multixact.c:2705-2831