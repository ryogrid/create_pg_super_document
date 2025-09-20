# TransferPredicateLocksToHeapRelation

## Location
[src/backend/storage/lmgr/predicate.c:3113-3133](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L3113-L3133)

## Overview
Transfers all predicate locks from any granularity level on a given relation to a single coarse-grained relation lock on the corresponding heap relation.

## Definition

```c
void
TransferPredicateLocksToHeapRelation(Relation relation)
```
## Detailed Description
TransferPredicateLocksToHeapRelation is a high-level wrapper function that provides a clean interface for transferring predicate locks during DDL operations. It serves as the public API for lock consolidation operations where fine-grained locks (tuple-level, page-level, or index locks) need to be replaced with a single relation-level lock on the heap.

The function delegates the actual work to DropAllPredicateLocksFromTable with the transfer flag set to true, ensuring that all existing serialization constraints are preserved while simplifying the lock structure. This is particularly important during operations that restructure or remove database objects, where maintaining the original fine-grained locks would be impossible or inappropriate.

This consolidation approach ensures that serializable transaction isolation guarantees are maintained even when the underlying physical structure changes, by using the most coarse-grained lock that still provides the necessary conflict detection.

## Parameters / Member Variables
- : The relation (heap table or index) whose predicate locks should be transferred to the heap relation

## Dependencies
- Functions called/Symbols referenced:
  - [DropAllPredicateLocksFromTable](../D/DropAllPredicateLocksFromTable.md)
- Called from (representative examples):
  - [index_concurrently_set_dead](../i/index_concurrently_set_dead.md)
  - [index_drop](../i/index_drop.md)  
  - [reindex_index](../r/reindex_index.md)
  - [cluster_rel](../c/cluster_rel.md)
  - [ATRewriteTable](../A/ATRewriteTable.md)

## Notes and Other Information
- Public function - part of the external predicate locking API
- Wrapper around DropAllPredicateLocksFromTable with transfer=true
- Essential for DDL operations that modify or remove relations while maintaining serializable isolation
- Used during index operations (DROP INDEX, REINDEX), table clustering, and table rewrites
- Ensures that serialization conflicts are still detected even after structural changes to the database
- Part of PostgreSQL's Serializable Snapshot Isolation (SSI) implementation
- Critical for maintaining ACID properties during concurrent DDL and DML operations in serializable transactions