# PredicateLockRelation

## Location
[src/backend/storage/lmgr/predicate.c:2566-2588](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L2566-L2588)

## Overview
Acquires a predicate lock at the relation level for serializable transactions, providing coarse-grained locking for entire tables.

## Definition
void PredicateLockRelation(Relation relation, Snapshot snapshot)

## Detailed Description
This function provides the public interface for acquiring relation-level predicate locks in PostgreSQL's serializable snapshot isolation implementation. It serves as a high-level wrapper that handles the prerequisites for predicate locking before delegating to the core locking mechanism.

The function first checks if serialization is needed for the given relation and snapshot using SerializationNeededForRead, which handles cases like non-serializable transactions and temporary tables. If locking is needed, it constructs a PREDICATELOCKTARGETTAG for the relation using the database OID and relation OID, then calls PredicateLockAcquire to perform the actual lock acquisition. This relation-level lock represents the coarsest granularity in the predicate locking hierarchy and will automatically clear any finer-grained locks on the same relation.

## Parameters / Member Variables
- : Pointer to the Relation structure representing the table to be locked
- : Pointer to the Snapshot being used for the current operation

## Dependencies
- Functions called/Symbols referenced:
  - [SerializationNeededForRead](../S/SerializationNeededForRead.md)
  - SET_PREDICATELOCKTARGETTAG_RELATION
  - [PredicateLockAcquire](PredicateLockAcquire.md)
  - [PREDICATELOCKTARGETTAG](PREDICATELOCKTARGETTAG.md) (struct)
- Called from (representative examples):
  - [heap_beginscan](../h/heap_beginscan.md)
  - [index_beginscan_internal](../i/index_beginscan_internal.md)
  - [_bt_first](../b/_bt_first.md)
  - [_bt_endpoint](../b/_bt_endpoint.md)

## Notes and Other Information
- This is one of the main public entry points for predicate locking in PostgreSQL
- Automatically skips locking for non-serializable transactions and temporary tables
- [Relation](../R/Relation.md)-level locks provide the coarsest granularity and maximum concurrency reduction but minimum lock table overhead
- The function will cause any existing page-level or tuple-level predicate locks on the same relation to be cleaned up
- Commonly called at the beginning of table scans to establish read dependencies for serializable transactions
- Part of PostgreSQL's implementation of true serializable isolation level using predicate locking