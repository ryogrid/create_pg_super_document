# heap_lock_tuple

## Location
src/backend/access/heap/heapam.c: 4533 - 5230

## Overview
heap_lock_tuple is the core function responsible for acquiring shared or exclusive locks on heap tuples, handling complex concurrency control, transaction visibility, and MultiXact management in PostgreSQL.

## Definition


## Detailed Description
This function implements PostgreSQL's sophisticated tuple locking mechanism, managing concurrent access to individual rows. It handles multiple lock modes (KeyShare, Share, NoKeyExclusive, Exclusive), visibility checking, transaction conflict resolution, and MultiXact management. The function can optionally follow update chains to lock descendant tuples and implements various wait policies.

Key operations include:
1. Buffer management and visibility map optimization
2. Tuple visibility verification using HeapTupleSatisfiesUpdate
3. Conflict detection and resolution with existing lockers/updaters
4. MultiXact and single-transaction lock management
5. Wait policy enforcement (Block, Skip, Error)
6. Update chain following for comprehensive locking
7. Transaction information recording and WAL logging

## Parameters / Member Variables
- : Relation containing the tuple to lock
- : Heap tuple to lock (filled in on output)
- : Current command ID for visibility testing and storage
- : Lock mode (KeyShare, Share, NoKeyExclusive, Exclusive)
- : Behavior when lock unavailable (Block, Skip, Error)
- : If true, follow update chain to lock descendant tuples
- : Output parameter for buffer containing tuple (pinned but not locked)
- : Output parameter filled with failure details for non-success cases

## Dependencies
- Functions called/Symbols referenced:
  - ReadBuffer, LockBuffer (buffer management)
  - HeapTupleSatisfiesUpdate (visibility checking)
  - get_mxact_status_for_lock (MultiXact status mapping)
  - heap_acquire_tuplock (heavyweight tuple lock acquisition)
  - MultiXactIdWait, XactLockTableWait (waiting for transactions)
  - heap_lock_updated_tuple (follow update chain)
  - compute_new_xmax_infomask (compute new transaction information)
  - visibilitymap_pin, visibilitymap_clear (visibility map management)
  - UpdateXmaxHintBits (hint bit updates)
  - XLogInsert (WAL logging)
- Type references:
  - TM_Result (tuple manager result codes)
  - LockTupleMode (lock mode enumeration)
  - LockWaitPolicy (wait policy enumeration)
  - TM_FailureData (failure information structure)
  - MultiXactStatus (MultiXact member status)
- Called from (representative examples):
  - heapam_tuple_lock (heap access method interface)

## Notes and Other Information
- Implements PostgreSQL's sophisticated row-level locking with MultiXact support
- Handles four different lock modes with varying strength and conflict patterns
- Optimizes for common cases to avoid unnecessary waiting
- Manages both lightweight (infomask) and heavyweight (lock manager) tuple locks
- Supports update chain following for comprehensive row locking
- Implements visibility map optimizations for all-visible pages
- Includes comprehensive WAL logging for crash recovery
- Part of PostgreSQL's tuple manager providing MVCC concurrency control
- Critical for implementing SQL-standard isolation levels and FOR SHARE/UPDATE clauses
- Complex interaction with MultiXact system for handling multiple concurrent lockers