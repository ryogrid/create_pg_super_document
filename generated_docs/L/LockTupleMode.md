# LockTupleMode

## Location
[src/include/nodes/lockoptions.h:59-61](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/lockoptions.h#L59-L61)

## Overview
LockTupleMode is an enumeration that defines the different locking modes available for individual tuples in PostgreSQL, corresponding to the various FOR clauses in SELECT statements and the implicit locking behavior of UPDATE and DELETE operations.

## Definition


## Detailed Description
LockTupleMode defines the hierarchy of tuple-level locking modes in PostgreSQL's concurrency control system. These modes correspond directly to SQL locking clauses and determine the level of protection and conflict detection for individual table rows. The enum values are ordered from weakest to strongest locking mode, with each stronger mode conflicting with more operations than weaker modes.

The tuple locking system allows PostgreSQL to implement fine-grained concurrency control, enabling multiple transactions to work on the same table simultaneously while maintaining data consistency. Each mode serves specific use cases in transaction isolation and prevents different types of conflicts.

## Parameters / Member Variables
- : Weakest lock mode used by SELECT FOR KEY SHARE. Allows concurrent readers and most writers, but prevents deletion and key column updates by other transactions.
- : Used by SELECT FOR SHARE. Allows concurrent readers but prevents any updates or deletes by other transactions.
- : Used by SELECT FOR NO KEY UPDATE and UPDATE statements that don't modify key columns. Prevents concurrent updates and deletes, but allows key-share locks.
- : Strongest lock mode used by SELECT FOR UPDATE, UPDATE statements that modify key columns, and DELETE. Prevents all concurrent modifications except key-share locks.

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is an enum type definition)
- Called from (representative examples):
  - [heap_lock_tuple](../h/heap_lock_tuple.md) (src/backend/access/heap/heapam.c:4534)
  - [heap_update](../h/heap_update.md) (src/backend/access/heap/heapam.c:3202)
  - [ExecLockRows](../E/ExecLockRows.md) (src/backend/executor/nodeLockRows.c:83)
  - table_tuple_lock (src/include/access/tableam.h:1582)
  - [heapam_tuple_update](../h/heapam_tuple_update.md) (src/backend/access/heap/heapam_handler.c:318)

## Notes and Other Information
- The locking modes form a strict hierarchy where stronger locks conflict with more operations than weaker ones
- Key-share locks are designed to allow most concurrent operations while preventing tuple deletion and modification of key columns that could affect foreign key relationships
- The tuple locking mechanism is thoroughly documented in src/backend/access/heap/README.tuplock
- These modes integrate with PostgreSQL's multi-version concurrency control (MVCC) system to provide transaction isolation
- Lock conflicts are resolved according to the lock strength hierarchy and the transaction's wait policy (NOWAIT, SKIP LOCKED, or default blocking behavior)
- The enum is defined in src/include/nodes/lockoptions.h and is used throughout the storage and executor subsystems