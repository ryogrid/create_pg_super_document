# GetOldestNonRemovableTransactionId

## Location
[src/backend/storage/ipc/procarray.c:2005-2033](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L2005-L2033)

## Overview
GetOldestNonRemovableTransactionId returns the oldest transaction ID for which deleted tuples must be preserved in a given table, providing the cleanup horizon for VACUUM operations.

## Definition
```c
TransactionId
GetOldestNonRemovableTransactionId(Relation rel)
```

## Detailed Description
This function serves as the primary interface for determining vacuum cleanup horizons in PostgreSQL. It computes transaction visibility horizons and selects the appropriate one based on the relation type. The function ensures that VACUUM operations preserve deleted tuples that might still be visible to running transactions.

The function works by:
1. Computing all visibility horizons using ComputeXidHorizons()
2. Determining the relation's visibility horizon kind via GlobalVisHorizonKindForRel()
3. Returning the corresponding horizon from the computed results

Different relation types require different horizon strategies:
- Shared relations use the most conservative horizon (shared_oldest_nonremovable)
- Catalog relations use catalog_oldest_nonremovable for system consistency
- Regular data relations use data_oldest_nonremovable for optimal cleanup
- Temporary relations use temp_oldest_nonremovable for aggressive cleanup

## Parameters / Member Variables
- `rel`: Relation pointer for which to determine the cleanup horizon; if NULL, returns a horizon safe for all relations

## Dependencies
- Functions called/Symbols referenced:
  - [ComputeXidHorizons](../C/ComputeXidHorizons.md)
  - [GlobalVisHorizonKindForRel](GlobalVisHorizonKindForRel.md)
  - [ComputeXidHorizonsResult](../C/ComputeXidHorizonsResult.md) (struct type)
  - VISHORIZON_SHARED, VISHORIZON_CATALOG, VISHORIZON_DATA, VISHORIZON_TEMP (enum values)
  - InvalidTransactionId (fallback return value)
- Called from:
  - [heapam_index_build_range_scan](../h/heapam_index_build_range_scan.md)
  - [_bt_pendingfsm_finalize](../b/_bt_pendingfsm_finalize.md)
  - [acquire_sample_rows](../a/acquire_sample_rows.md)
  - [vacuum_get_cutoffs](../v/vacuum_get_cutoffs.md)
  - [vac_update_datfrozenxid](../v/vac_update_datfrozenxid.md)
  - [removable_cutoff](../r/removable_cutoff.md) (test module)

## Notes and Other Information
- This is the main entry point for VACUUM operations to determine what tuples can be safely removed
- The function is designed to be conservative: when in doubt, it preserves more tuples rather than risk removing visible ones
- The NULL relation case provides a safe horizon for operations that affect multiple or unknown relations
- The function includes a compiler warning prevention return statement, though all enum cases should be covered
- Critical for maintaining MVCC consistency and preventing premature tuple removal that could cause data corruption or incorrect query results