# vac_update_datfrozenxid

## Location
src/backend/commands/vacuum.c: 1585 - 1803

## Overview
Updates the database-wide frozen transaction ID and minimum MultiXactId values in pg_database by scanning all relations and finding the minimum values, then attempts to truncate transaction logs if advancement is possible.

## Definition


## Detailed Description
This function performs a critical database maintenance task by updating the datfrozenxid and datminmxid fields in the pg_database system catalog. The process involves:

1. **Database-level Locking**: Acquires an exclusive lock to prevent concurrent updates that could cause the values to move backward
2. **Minimum Value Calculation**: Scans all pg_class entries to find the minimum relfrozenxid and relminmxid values across all relations
3. **Safety Validation**: Checks for "future" transaction IDs that might indicate corruption and abandons the operation if found
4. **Database Tuple Update**: Uses in-place updates to modify the pg_database tuple, avoiding transaction semantic issues similar to vac_update_relstats
5. **Log Truncation**: Attempts to truncate pg_xact and pg_multixact logs if advancement was achieved

The function employs conservative initialization values and validates all discovered transaction IDs to ensure database consistency. It uses the same in-place update mechanism as vac_update_relstats to avoid leaving dead tuples in system catalogs.

## Parameters / Member Variables
This function takes no parameters and operates on the current database (MyDatabaseId).

## Dependencies
- Functions called/Symbols referenced:
  - LockDatabaseFrozenIds
  - GetOldestNonRemovableTransactionId
  - GetOldestMultiXactId
  - ReadNextTransactionId
  - ReadNextMultiXactId
  - systable_beginscan
  - systable_getnext
  - systable_inplace_update_begin
  - systable_inplace_update_finish
  - systable_inplace_update_cancel
  - TransactionIdIsNormal
  - TransactionIdPrecedes
  - MultiXactIdIsValid
  - MultiXactIdPrecedes
  - ForceTransactionIdLimitUpdate
  - vac_truncate_clog
  - heap_freetuple
- Called from (representative examples):
  - vacuum
  - do_autovacuum

## Notes and Other Information
- Only considers relations that can hold unfrozen XIDs (RELKIND_RELATION, RELKIND_MATVIEW, RELKIND_TOASTVALUE)
- Implements a "chicken out" mechanism when bogus (future) transaction IDs are detected to prevent premature log truncation
- The exclusive database lock prevents race conditions where multiple backends might compute different minimum values
- Transaction ID advancement is conditional - the function will not move datfrozenxid backward unless the current value appears corrupt
- Some table access methods may not require per-relation XID/MultiXact horizons, so Invalid values are handled appropriately
- The function triggers log truncation even when ForceTransactionIdLimitUpdate() indicates stale shared XID-wrap-limit information
- Uses sequential scan of pg_class since no suitable index exists for finding minimum transaction ID values