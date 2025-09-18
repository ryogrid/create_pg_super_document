# vac_update_relstats

## Location
src/backend/commands/vacuum.c: 1409 - 1584

## Overview
Updates the whole-relation statistics stored in the pg_class system catalog row for a given relation, including both heap and index relations, using in-place tuple updates to avoid transaction semantics issues during vacuum operations.

## Definition


## Detailed Description
This function updates the statistical information for a relation in its pg_class tuple. It deliberately violates transaction semantics by using in-place updates to overwrite the existing tuple data directly. This approach is necessary because:

1. **Vacuum Efficiency**: If regular tuple updates were used, vacuuming pg_class itself would be inefficient since most tuples would become obsoleted during a vacuum cycle.

2. **PROC_IN_VACUUM Safety**: When in lazy VACUUM mode with PROC_IN_VACUUM set, regular updates could cause issues where concurrent vacuum operations might incorrectly delete tuples.

The function updates three types of information:
- **Statistical Data**: Always updated (relpages, reltuples, relallvisible)
- **DDL Flags**: Updated only when not in an outer transaction (relhasindex, relhasrules, relhastriggers)
- **Transaction IDs**: Updated with proper validation (relfrozenxid, relminmxid)

Special handling is provided for "future" transaction IDs that appear corrupt, which are overwritten with new valid values.

## Parameters / Member Variables
- : The relation whose statistics are being updated
- : New number of pages in the relation
- : New count of live tuples in the relation
- : New count of all-visible pages for visibility map
- : Whether the relation currently has any indexes
- : New frozen transaction ID, or InvalidTransactionId if no update needed
- : New minimum MultiXactId, or InvalidMultiXactId if no update needed
- : Output parameter indicating if relfrozenxid was actually updated
- : Output parameter indicating if relminmxid was actually updated
- : Whether this operation is within an outer transaction (affects DDL flag updates)

## Dependencies
- Functions called/Symbols referenced:
  - [systable_inplace_update_begin](../s/systable_inplace_update_begin.md)
  - [systable_inplace_update_finish](../s/systable_inplace_update_finish.md)
  - [systable_inplace_update_cancel](../s/systable_inplace_update_cancel.md)
  - TransactionIdIsNormal
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
  - ReadNextTransactionId
  - MultiXactIdIsValid
  - [MultiXactIdPrecedes](../M/MultiXactIdPrecedes.md)
  - [ReadNextMultiXactId](../R/ReadNextMultiXactId.md)
- Called from (representative examples):
  - [heap_vacuum_rel](../h/heap_vacuum_rel.md)
  - [update_relstats_all_indexes](../u/update_relstats_all_indexes.md)
  - [do_analyze_rel](../d/do_analyze_rel.md)

## Notes and Other Information
- This function is shared by both VACUUM and ANALYZE operations
- Only updates DDL flags (hasindex, hasrules, hastriggers) when not in an outer transaction to avoid incorrect flag clearing during rollbacks
- Provides warnings when overwriting seemingly corrupt "future" transaction IDs
- The in-place update mechanism requires the statistics being updated to be fixed-size, not-null columns
- Transaction ID validation prevents relfrozenxid from going backwards unless the stored value appears to be corrupt ("in the future")