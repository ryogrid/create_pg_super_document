# BulkInsertStateData

## Location
src/include/access/hio.h: 29 - 51

## Overview
BulkInsertStateData is a structure that maintains state information for bulk insert operations in PostgreSQL, providing optimization mechanisms for efficient page management during large-scale data insertions.

## Definition


## Detailed Description
BulkInsertStateData is a private data structure used internally by heapam.c and hio.c to optimize bulk insert operations. This structure maintains critical state information that enables PostgreSQL to efficiently manage page allocation and buffer access during large insert operations.

The structure serves as a coordination mechanism between the heap access methods and heap I/O subsystems, tracking the current insertion target page and managing a pool of pre-allocated pages for future insertions. This design reduces the overhead of frequent page allocation requests during bulk operations.

Key optimization features include:
- Maintaining a current insertion target buffer with an extra pin to avoid repeated buffer lookups
- Tracking ranges of free pages (next_free to last_free) that were available during the last table extension
- Recording extension history (already_extended_by) to make adaptive decisions about future extensions
- Using a dedicated BULKWRITE buffer access strategy to optimize cache behavior

## Parameters / Member Variables
- : BufferAccessStrategy object configured for BULKWRITE operations, managing buffer cache behavior during bulk inserts
- : Buffer identifier for the page currently being used as the insertion target; InvalidBuffer when no page is pinned
- : BlockNumber indicating the start of the range of pages that were free during the last table extension
- : BlockNumber indicating the end of the range of pages that were free during the last table extension
- : Counter tracking the total number of pages this bulk insert operation has extended the relation by, used for adaptive extension strategies

## Dependencies
- Functions called/Symbols referenced:
  - BufferAccessStrategy (from src/include/storage/buf.h)
  - Buffer (from src/include/storage/buf.h)
  - BlockNumber (from src/include/storage/block.h)

- Called from (representative examples):
  - GetBulkInsertState (src/backend/access/heap/heapam.c:1975)
  - BulkInsertState (typedef in src/include/access/heapam.h:44)
  - table_tuple_insert (src/include/access/tableam.h:1404)
  - table_tuple_insert_speculative (src/include/access/tableam.h:1424)
  - table_multi_insert (src/include/access/tableam.h:1459)

## Notes and Other Information
- This structure is marked as private to heapam.c and hio.c, indicating it's an internal implementation detail not exposed to higher-level code
- The current_buf member requires special handling: when it's not InvalidBuffer, an extra pin is held on that buffer
- The free page range (next_free to last_free) requires validation before use, as pages might have been allocated by other processes since the last extension
- The comment suggests that the bulk extension state members might eventually be moved to RelationData structure for better organization
- The already_extended_by counter enables adaptive behavior: bulk inserts that have already extended significantly can be more aggressive in future extensions
- This structure is part of PostgreSQL's table access method (AM) infrastructure, supporting pluggable storage engines