# GetOldestMultiXactId

## Location
src/backend/access/transam/multixact.c: 2652 - 2704

## Overview
Returns the oldest MultiXactId that could still be considered live by any running transaction, used for determining safe points for vacuum operations and SLRU management.

## Definition
MultiXactId GetOldestMultiXactId(void)

## Detailed Description
This function determines the oldest MultiXactId that might still be referenced by any active transaction in the system. It examines all entries in the OldestMemberMXactId and OldestVisibleMXactId arrays to find the minimum valid value across all slots. If no valid entries exist, it returns the next MultiXactId to be assigned.

The function is critical for vacuum operations and SLRU management decisions. While it's not safe to truncate MultiXact SLRU segments based solely on this value, it can be used to set relminmxid for tables that VACUUM knows have no remaining MXIDs older than this value. The function handles wraparound conditions carefully by ensuring that nextMXact is normalized to a valid range.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire (MultiXactGenLock, LW_SHARED)
  - MultiXactIdIsValid
  - [MultiXactIdPrecedes](../M/MultiXactIdPrecedes.md)
  - LWLockRelease
  - FirstMultiXactId
  - MaxOldestSlot
  - OldestMemberMXactId array
  - OldestVisibleMXactId array
- Called from (representative examples):
  - [heapam_relation_set_new_filelocator](../h/heapam_relation_set_new_filelocator.md)
  - [vacuum_get_cutoffs](../v/vacuum_get_cutoffs.md)
  - [vac_update_datfrozenxid](../v/vac_update_datfrozenxid.md)

## Notes and Other Information
- Uses shared locking on MultiXactGenLock for safe concurrent access
- Handles wraparound conditions by normalizing nextMXact to valid range
- Examines both OldestMemberMXactId and OldestVisibleMXactId arrays
- Critical for VACUUM operations and determining safe truncation points
- Does not guarantee that returned value is safe for SLRU truncation
- Safe for setting relminmxid values in vacuum operations
- Returns the most conservative (oldest) valid MultiXactId found