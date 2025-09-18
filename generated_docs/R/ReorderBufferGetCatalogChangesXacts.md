# ReorderBufferGetCatalogChangesXacts

## Location
src/backend/replication/logical/reorderbuffer.c: 3568 - 3602

## Overview
Returns a sorted array of transaction IDs that have made catalog changes, used for snapshot serialization and logical replication processing.

## Definition
TransactionId *ReorderBufferGetCatalogChangesXacts(ReorderBuffer *rb)

## Detailed Description
This function creates and returns a dynamically allocated array containing the transaction IDs of all transactions that have been marked as having catalog changes. The array is sorted using the xidComparator function to ensure consistent ordering. This is primarily used during snapshot serialization to record which transactions modified system catalogs, allowing proper handling during logical replication replay. The function iterates through the catalog changes transaction list maintained by the reorder buffer and extracts the transaction IDs.

## Parameters / Member Variables
- rb: Pointer to the ReorderBuffer structure containing the catalog changes transaction list

## Dependencies
- Functions called/Symbols referenced:
  - [dclist_count](../d/dclist_count.md)
  - dclist_foreach
  - dclist_container
  - [palloc](../p/palloc.md)
  - qsort
  - [xidComparator](../x/xidComparator.md)
  - rbtxn_has_catalog_changes
  - Assert
- Called from (representative examples):
  - [SnapBuildSerialize](../S/SnapBuildSerialize.md)

## Notes and Other Information
- Returns NULL if no transactions have catalog changes
- The caller is responsible for freeing the returned array using pfree
- The returned array is sorted in transaction ID order for consistent processing
- Used primarily during snapshot serialization to maintain catalog change information across restarts
- The function includes assertions to verify data consistency
- Essential component of PostgreSQL's logical replication infrastructure for handling catalog modifications