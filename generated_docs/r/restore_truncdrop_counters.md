# restore_truncdrop_counters

## Location
src/backend/utils/activity/pgstat_relation.c: 978 - 986

## Overview
Restores previously saved insert/update/delete counter values when a transaction containing truncate or drop operations is rolled back, ensuring statistics consistency.

## Definition
```c
static void restore_truncdrop_counters(PgStat_TableXactStatus *trans)
```

## Detailed Description
This function implements the restoration half of PostgreSQL's statistics rollback mechanism for destructive table operations. When a transaction or subtransaction that performed truncate/drop operations is aborted, this function restores the table's statistics counters to their pre-operation state.

The function operates with simple but critical logic:
1. Checks if the transaction had truncate/drop operations (via the `truncdropped` flag)
2. If so, restores the three main counter types:
   - `tuples_inserted` from `inserted_pre_truncdrop`
   - `tuples_updated` from `updated_pre_truncdrop` 
   - `tuples_deleted` from `deleted_pre_truncdrop`

This restoration ensures that if a transaction containing destructive operations fails and rolls back, the statistics remain as if those operations never occurred, maintaining the ACID properties for PostgreSQL's statistics system.

## Parameters / Member Variables
- `trans`: Pointer to the transaction-specific table statistics status structure containing both current and saved counter values

## Dependencies
- Functions called/Symbols referenced:
  - `PgStat_TableXactStatus`: Transaction-specific table statistics structure containing the counter fields and saved values
- Called from (representative examples):
  - `AtEOXact_PgStat_Relations`: During transaction abort cleanup
  - `AtEOSubXact_PgStat_Relations`: During subtransaction abort cleanup

## Notes and Other Information
- The function is static and only used internally within the statistics relation module
- Works in conjunction with `save_truncdrop_counters` to provide complete rollback capability for statistics
- Only performs restoration if the `truncdropped` flag is set, indicating that counters were previously saved
- This is a critical component of PostgreSQL's transactional statistics system, ensuring that statistics remain consistent with the actual table state after rollbacks
- Handles both main transaction aborts and subtransaction (savepoint) rollbacks uniformly
- The simplicity of the function belies its importance in maintaining statistics integrity across complex transaction scenarios