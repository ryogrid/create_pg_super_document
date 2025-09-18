# PrefetchBuffer

## Location
[src/backend/storage/buffer/bufmgr.c:638-668](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L638-L668)

## Overview
High-level interface for prefetching relation blocks that routes to appropriate buffer pool implementation based on relation type.

## Definition
PrefetchBufferResult PrefetchBuffer(Relation reln, ForkNumber forkNum, BlockNumber blockNum)

## Detailed Description
PrefetchBuffer serves as the main entry point for block prefetching operations in PostgreSQL. It abstracts the complexity of different buffer pool types by examining the relation characteristics and routing the request to either local or shared buffer prefetching implementations. The function enforces security restrictions by preventing access to temporary tables from other sessions, ensuring data isolation between database sessions.

The function supports three possible outcomes: finding blocks already cached (with buffer hint), successfully initiating asynchronous I/O, or determining that prefetching is not possible due to system limitations or configuration. This design allows callers to optimize their access patterns while gracefully handling cases where prefetching provides no benefit.

## Parameters / Member Variables
- reln: Relation structure containing metadata about the target relation
- forkNum: Fork identifier specifying which fork of the relation to access
- blockNum: Block number within the specified fork to prefetch

## Dependencies
- Functions called/Symbols referenced:
  - RelationIsValid: Validates relation structure
  - BlockNumberIsValid: Validates block number
  - RelationUsesLocalBuffers: Determines buffer pool type
  - RELATION_IS_OTHER_TEMP: Checks for cross-session temporary table access
  - PrefetchLocalBuffer: Handles local buffer prefetching
  - [PrefetchSharedBuffer](PrefetchSharedBuffer.md): Handles shared buffer prefetching
  - RelationGetSmgr: Gets storage manager handle
- Called from (representative examples):
  - [index_delete_prefetch_buffer](../i/index_delete_prefetch_buffer.md): Index tuple deletion optimization
  - [count_nondeletable_pages](../c/count_nondeletable_pages.md): Vacuum operation optimization
  - [BitmapPrefetch](../B/BitmapPrefetch.md): Bitmap heap scan prefetching

## Notes and Other Information
- Temporary tables from other sessions are explicitly blocked for security reasons
- The function automatically determines whether to use local or shared buffer pools
- Return value provides hints for optimization but requires validation by caller
- Prefetching is advisory and may not always result in actual I/O initiation
- Designed to work seamlessly with existing ReadBuffer operations