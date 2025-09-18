# ReindexIsProcessingIndex

## Location
[src/backend/catalog/index.c:4079-4089](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/index.c#L4079-L4089)

## Overview
The  function determines whether an index is currently being reindexed or is pending reindex, serving as a comprehensive check for index unavailability during reindex operations.

## Definition


## Detailed Description
This function provides a comprehensive check to determine if an index should be considered unavailable due to reindexing operations. It checks two conditions: whether the index is currently being actively reindexed (by comparing against ) or whether it's in the pending reindex list (checked via ). This broader check is essential for PostgreSQL's reindex coordination system, ensuring that operations avoid using indexes that are either currently being rebuilt or marked as needing rebuild.

The function is crucial for maintaining database consistency during reindex operations, particularly in scenarios where multiple indexes need to be rebuilt sequentially or when system catalog operations need to avoid using inconsistent indexes.

## Parameters / Member Variables
- : Object identifier of the index to check for reindexing status

## Dependencies
- Functions called/Symbols referenced:
  - [list_member_oid](../l/list_member_oid.md): Checks if the index OID exists in the pending reindex list
  - currentlyReindexedIndex: Global variable tracking the currently active reindexed index
  - pendingReindexedIndexes: Global list of indexes awaiting reindex
- Called from (representative examples):
  - [systable_beginscan](../s/systable_beginscan.md): System catalog scanning operations
  - [systable_beginscan_ordered](../s/systable_beginscan_ordered.md): Ordered system catalog scanning
  - [CatalogIndexInsert](../C/CatalogIndexInsert.md): System catalog index insertion operations
  - [reindex_relation](../r/reindex_relation.md): Verification during relation reindexing
  - RELATION_CHECKS: Index availability validation

## Notes and Other Information
- Returns true if the index is either currently being reindexed or is pending reindex
- More comprehensive than ReindexIsCurrentlyProcessingIndex, which only checks active reindexing
- Critical for system catalog operations to avoid using inconsistent indexes
- Used by the index access methods to determine index availability
- Part of the broader reindex coordination mechanism that includes SetReindexPending and RemoveReindexPending
- Essential for maintaining transactional consistency during complex reindex operations