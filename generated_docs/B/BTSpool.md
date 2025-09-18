# BTSpool

## Location
[src/backend/access/nbtree/nbtsort.c:80-87](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsort.c#L80-L87)

## Overview
BTSpool is a status record structure used during the spooling and sorting phase of B-tree index construction in PostgreSQL. It maintains the necessary state information for sorting tuples during index builds, with special considerations for uniqueness constraints and dead tuple handling.

## Definition


## Detailed Description
BTSpool serves as a wrapper structure that encapsulates all the necessary information required for sorting tuples during B-tree index construction. The structure is designed to handle both regular and unique index builds, with special provisions for handling dead tuples in unique indexes. During parallel index builds, multiple BTSpool instances may be created to coordinate work across different processes. The structure integrates with PostgreSQL's tuplesort module to provide efficient external sorting capabilities when memory constraints require spilling to disk.

## Parameters / Member Variables
- : Pointer to Tuplesortstate containing the actual sorting state and operations managed by tuplesort.c
- : Relation pointer to the heap table being indexed
- : Relation pointer to the B-tree index being constructed
- : Boolean flag indicating whether the index enforces uniqueness constraints
- : Boolean flag controlling whether NULL values are considered distinct for uniqueness checking

## Dependencies
- Functions called/Symbols referenced:
  - Tuplesortstate
- Called from (representative examples):
  - [BTBuildState](BTBuildState.md)
  - [_bt_spools_heapscan](../b/_bt_spools_heapscan.md)
  - [_bt_spooldestroy](../b/_bt_spooldestroy.md)
  - [_bt_spool](../b/_bt_spool.md)
  - [_bt_leafbuild](../b/_bt_leafbuild.md)
  - _bt_load
  - [_bt_begin_parallel](../b/_bt_begin_parallel.md)
  - [_bt_leader_participate_as_worker](../b/_bt_leader_participate_as_worker.md)
  - [_bt_parallel_build_main](../b/_bt_parallel_build_main.md)
  - [_bt_parallel_scan_and_sort](../b/_bt_parallel_scan_and_sort.md)

## Notes and Other Information
- The comment in the source indicates that there may be two BTSpool instances in certain scenarios, particularly when dealing with uniqueness-checking requirements involving dead tuples
- This structure is central to PostgreSQL's B-tree index construction algorithm, which uses external sorting to handle large datasets that don't fit in memory
- The structure is defined in src/backend/access/nbtree/nbtsort.c and is primarily used within the nbtree access method implementation
- The nulls_not_distinct field relates to the SQL standard's treatment of NULL values in unique constraints, where NULLs can be considered either distinct or not distinct depending on the index definition