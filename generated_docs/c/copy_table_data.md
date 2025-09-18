# copy_table_data

## Location
[src/backend/commands/cluster.c:814-1060](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/cluster.c#L814-L1060)

## Overview
Performs the physical copying of table data from an old heap to a new heap, handling tuple visibility, freezing, and transaction management during table reorganization operations.

## Definition


## Detailed Description
The `copy_table_data` function is responsible for the actual data transfer during table clustering and rewriting operations. It handles complex aspects of PostgreSQL's MVCC system including:

1. **Data Transfer**: Copies all visible tuples from the old table to the new table
2. **Ordering**: Uses either index scan (for clustering) or sequential scan with optional sorting
3. **Transaction Management**: Computes appropriate freeze cutoff points for transaction IDs
4. **TOAST Handling**: Manages TOAST table relationships and decides between content vs. link swapping
5. **Statistics**: Updates table statistics (relpages, reltuples) in pg_class catalog

The function is access method (AM) agnostic, delegating the actual copying to AM-specific functions while handling the generic coordination tasks.

## Parameters / Member Variables
- `OIDNewHeap`: OID of the destination table to copy data into
- `OIDOldHeap`: OID of the source table to copy data from  
- `OIDOldIndex`: OID of index to use for ordering (InvalidOid for physical order)
- `verbose`: Boolean flag controlling logging verbosity
- `pSwapToastByContent`: Output parameter indicating whether TOAST swap should be by content
- `pFreezeXid`: Output parameter receiving the transaction ID used as freeze cutoff
- `pCutoffMulti`: Output parameter receiving the MultiXactId used as cutoff

## Dependencies
- Functions called/Symbols referenced:
  - table_open/table_close: Opens and closes table relations
  - [index_open](../i/index_open.md)/index_close: Opens and closes index relations  
  - [LockRelationOid](../L/LockRelationOid.md): Locks TOAST table to prevent autovacuum interference
  - [vacuum_get_cutoffs](../v/vacuum_get_cutoffs.md): Computes freeze and MultiXact cutoff values
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md): Compares transaction IDs for cutoff calculations
  - [plan_cluster_use_sort](../p/plan_cluster_use_sort.md): Determines whether to use sort vs index scan
  - table_relation_copy_for_cluster: AM-specific data copying function
  - SearchSysCacheCopy1: Retrieves catalog information
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md): Updates system catalog entries
  - CommandCounterIncrement: Makes catalog changes visible
- Called from (representative examples):
  - [rebuild_relation](../r/rebuild_relation.md): Main table rebuilding function
  - RelToCluster: Cluster processing workflow

## Notes and Other Information
- Handles both clustered (index-ordered) and non-clustered (physical order) copying
- Manages TOAST table locking to prevent autovacuum race conditions
- Computes aggressive freeze cutoffs since the entire table is being rewritten
- Uses planner cost estimates to choose between index scan and sort methods
- Updates table statistics in pg_class to reflect the new table's characteristics  
- Preserves TOAST value OIDs when doing content-based TOAST swapping
- Provides detailed logging of the operation's progress and results
- Critical for maintaining data consistency during table reorganization operations