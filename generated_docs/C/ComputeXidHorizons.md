# ComputeXidHorizons

## Location
src/backend/storage/ipc/procarray.c: 1735 - 1970

## Overview
ComputeXidHorizons calculates various transaction ID horizons that determine safe vacuum boundaries for different table types and replication requirements.

## Definition
```c
static void ComputeXidHorizons(ComputeXidHorizonsResult *h)
```

## Detailed Description
This function is the core engine for determining transaction visibility horizons in PostgreSQL, computing multiple different XID boundaries that control when tuples can be safely removed by vacuum operations. It serves as the foundation for various wrapper functions like GetOldestNonRemovableTransactionId() and GetReplicationHorizons().

**Key computed horizons:**
1. **oldest_considered_running**: Oldest XID that might be considered running by any backend
2. **shared_oldest_nonremovable**: Oldest XID that must be preserved in shared tables
3. **data_oldest_nonremovable**: Oldest XID that must be preserved in current database tables
4. **catalog_oldest_nonremovable**: Oldest XID that must be preserved in catalog tables (for logical decoding)
5. **temp_oldest_nonremovable**: Oldest XID that must be preserved in temporary tables
6. **slot_xmin/slot_catalog_xmin**: Replication slot constraints

**Algorithm details:**
1. Initializes all horizons to latestCompletedXid + 1 as a conservative starting point
2. Scans all active processes to find their xmin and xid values
3. Applies different rules based on process status flags:
   - Skips VACUUM and logical decoding processes for certain horizons
   - Includes all databases for shared tables
   - Filters by current database for regular tables
   - Handles special cases like PROC_AFFECTS_ALL_HORIZONS
4. Incorporates replication slot requirements
5. Adjusts for recovery mode using KnownAssignedXids
6. Ensures consistency across all computed horizons

**Important considerations:**
- Values can move backwards between calls due to changing transaction patterns
- Conservative approach ensures safety even with concurrent activity
- Different table types (shared, regular, catalog, temporary) have different requirements
- Replication slots can force preservation of much older data

## Parameters / Member Variables
- `h`: Output structure (ComputeXidHorizonsResult) containing all computed horizon values:
  - `latest_completed`: Most recently completed transaction
  - `oldest_considered_running`: Oldest XID that might be running
  - `shared_oldest_nonremovable`: Horizon for shared tables
  - `data_oldest_nonremovable`: Horizon for database-specific tables
  - `catalog_oldest_nonremovable`: Horizon for catalog tables
  - `temp_oldest_nonremovable`: Horizon for temporary tables
  - `slot_xmin/slot_catalog_xmin`: Replication slot constraints

## Dependencies
- Functions called/Symbols referenced:
  - RecoveryInProgress (to check if in recovery mode)
  - XidFromFullTransactionId (for transaction ID conversion)
  - TransactionIdAdvance (to increment transaction IDs)
  - TransactionIdOlder (to find minimum between two XIDs)
  - TransactionIdPrecedesOrEquals (for XID ordering verification)
  - KnownAssignedXidsGetOldestXmin (for recovery mode oldest XID)
  - GlobalVisUpdateApply (to update global visibility state)
- Called from:
  - GetOldestNonRemovableTransactionId (VACUUM operations)
  - GetOldestTransactionIdConsideredRunning (pg_subtrans truncation)
  - GetReplicationHorizons (hot standby feedback)
  - GlobalVisUpdate (global visibility state management)

## Notes and Other Information
- Critical function for vacuum efficiency and data safety
- Processes with PROC_IN_VACUUM or PROC_IN_LOGICAL_DECODING flags are handled specially
- Recovery mode requires different logic using KnownAssignedXids instead of local process array
- Temporary table horizon only considers current backend's transactions
- Replication slots can significantly impact computed horizons by requiring preservation of older data
- Extensive assertions verify consistency relationships between computed horizons
- Updates global approximate horizons for performance optimization
- The computed values represent conservative estimates - anything older is guaranteed safe to remove