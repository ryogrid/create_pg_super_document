# SnapBuildXidHasCatalogChanges

## Location
src/backend/replication/logical/snapbuild.c: 1244 - 1273

## Overview
Determines whether a given transaction has modified system catalogs by checking both the reorder buffer and the snapshot builder's catalog change tracking structures.

## Definition

```c
static inline bool
SnapBuildXidHasCatalogChanges(SnapBuild *builder, TransactionId xid,
							  uint32 xinfo)
```
## Detailed Description
SnapBuildXidHasCatalogChanges is a critical function for logical replication that determines whether a transaction has made changes to PostgreSQL's system catalogs. This determination is essential because catalog-modifying transactions require special handling in snapshot building to maintain consistency during logical replication.

The function employs a multi-layered approach to detect catalog changes:

1. **Reorder Buffer Check**: First consults the reorder buffer to see if the transaction is already known to have catalog changes. This is the primary and most reliable source.

2. **Invalidation Information Requirement**: Transactions that modify catalogs must have invalidation information (XACT_XINFO_HAS_INVALS flag). If this flag is not set, the transaction cannot have modified catalogs.

3. **Catalog Change Array Search**: Performs a binary search on the builder's sorted catalog change XID array to determine if the transaction is recorded as having made catalog modifications.

This function is inline for performance reasons since it's called frequently during transaction processing in logical replication scenarios.

## Parameters / Member Variables
- : The SnapBuild context containing catalog change tracking information
- : Transaction ID to check for catalog modifications
- : Transaction information flags, particularly checking for XACT_XINFO_HAS_INVALS

## Dependencies
- Functions called/Symbols referenced:
  - ReorderBufferXidHasCatalogChanges
  - bsearch (for searching the catchange.xip array)
  - xidComparator (comparison function for binary search)
  - XACT_XINFO_HAS_INVALS (flag constant)
- Called from (representative examples):
  - SnapBuildCommitTxn (multiple times for main transaction and subtransactions)

## Notes and Other Information
- Declared as static inline for performance optimization since it's called frequently
- Uses binary search for efficient lookup in the catalog change XID array
- The catchange.xip array must be kept sorted for binary search to work correctly
- Critical for determining which transactions need historical snapshot handling
- The XACT_XINFO_HAS_INVALS flag serves as a quick filter to avoid unnecessary array searches
- Part of the logical replication infrastructure that ensures schema changes are properly handled during replication setup