# hashbuild

## Location
src/backend/access/hash/hash.c: 115 - 200

## Overview
Builds a new hash index by scanning the heap relation and inserting all tuples into the newly created hash index structure.

## Definition
```c
IndexBuildResult *hashbuild(Relation heap, Relation index, IndexInfo *indexInfo)
```

## Detailed Description
The hashbuild function is responsible for creating a complete hash index from scratch. It performs several key operations: estimates the number of tuples in the heap relation, initializes the hash index metadata and initial buckets, and then scans the entire heap to insert all valid tuples into the index.

The function includes an optimization for large indexes that might not fit in memory. If the estimated index size exceeds maintenance_work_mem or the number of available buffers, it uses a spooling mechanism to sort tuples by their expected bucket number before insertion. This prevents thrashing when the index is larger than available RAM by improving locality of access.

The build process uses the table_index_build_scan function with hashbuildCallback to process each tuple during the heap scan. Progress is tracked and reported through the PostgreSQL progress reporting system.

## Parameters / Member Variables
- `heap`: The heap relation being indexed
- `index`: The hash index relation being built
- `indexInfo`: Index configuration and metadata information

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetNumberOfBlocks
  - [estimate_rel_size](../e/estimate_rel_size.md)
  - [_hash_init](_hash_init.md)
  - [_h_spoolinit](_h_spoolinit.md)
  - [table_index_build_scan](../t/table_index_build_scan.md)
  - [hashbuildCallback](hashbuildCallback.md)
  - [pgstat_progress_update_param](../p/pgstat_progress_update_param.md)
  - [_h_indexbuild](_h_indexbuild.md)
  - [_h_spooldestroy](_h_spooldestroy.md)
- Called from:
  - [hashhandler](hashhandler.md) (as amroutine->ambuild callback)
  - Index creation system

## Notes and Other Information
- Ensures the index relation is empty before building (throws ERROR if not)
- Uses a sophisticated sorting strategy for large indexes to prevent memory thrashing
- The sort threshold is determined by maintenance_work_mem and buffer pool size
- Temporary relations use NLocBuffer instead of NBuffers for the threshold calculation
- Returns statistics including the number of heap tuples scanned and index tuples created
- The function handles both sorted and unsorted insertion paths depending on index size