# cluster_rel

## Location
[src/backend/commands/cluster.c:311-499](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/cluster.c#L311-L499)

## Overview
The core function that performs the physical clustering of a single relation by rebuilding it in the order of a specified index, preserving the original table's OID and metadata.

## Definition


## Detailed Description
cluster_rel implements the low-level clustering operation for a single table. It creates a new table with tuples physically ordered according to the specified index, then swaps the relfilenumbers to preserve the original table's OID, grants, inheritance relationships, and references.

The function handles several important aspects:
1. **Security**: Switches to the table owner's user ID to ensure index functions execute with proper privileges
2. **Validation**: Performs extensive checks to ensure the operation is valid and safe
3. **Progress reporting**: Updates PostgreSQL's progress reporting system
4. **Transaction safety**: Handles both single-transaction and multi-transaction scenarios
5. **Lock management**: Acquires and manages exclusive locks appropriately

The clustering process involves calling rebuild_relation(), which does the heavy lifting of creating the new table structure and moving data. After rebuilding, indexes are recreated via REINDEX for optimal performance.

When indexOid is InvalidOid, this function implements VACUUM FULL functionality instead of clustering.

## Parameters / Member Variables
- : OID of the table to be clustered
- : OID of the index to cluster on, or InvalidOid for VACUUM FULL operation
- : ClusterParams structure containing options like VERBOSE, RECHECK, and RECHECK_ISCLUSTERED flags

## Dependencies
- Functions called/Symbols referenced:
  - [try_relation_open](../t/try_relation_open.md)/relation_close
  - [check_index_is_clusterable](check_index_is_clusterable.md)
  - [rebuild_relation](../r/rebuild_relation.md)
  - [GetUserIdAndSecContext](../G/GetUserIdAndSecContext.md)/SetUserIdAndSecContext
  - [pgstat_progress_start_command](../p/pgstat_progress_start_command.md)/pgstat_progress_end_command
  - [TransferPredicateLocksToHeapRelation](../T/TransferPredicateLocksToHeapRelation.md)
  - [CheckTableNotInUse](../C/CheckTableNotInUse.md)
  - SearchSysCacheExists1
  - [get_index_isclustered](../g/get_index_isclustered.md)
- Called from (representative examples):
  - [cluster](cluster.md)
  - [cluster_multiple_rels](cluster_multiple_rels.md)
  - [vacuum_rel](../v/vacuum_rel.md)

## Notes and Other Information
- Prevents clustering of shared catalogs (except for VACUUM FULL) to avoid indisclustered marking issues across databases
- Rejects operations on temporary tables from other sessions due to buffer manager limitations
- Handles materialized views by quietly ignoring unpopulated ones during multi-relation operations
- Promotes predicate locks to relation level since tuple locations change during clustering
- Uses security-restricted operations to prevent privilege escalation during index function execution
- The table is closed by rebuild_relation(), not by this function directly