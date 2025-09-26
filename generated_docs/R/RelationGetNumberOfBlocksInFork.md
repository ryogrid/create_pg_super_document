# RelationGetNumberOfBlocksInFork

## Location
[src/backend/storage/buffer/bufmgr.c:3912-3943](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L3912-L3943)

## Overview
RelationGetNumberOfBlocksInFork determines the current number of pages in a specified relation fork, handling different access methods and storage types appropriately.

## Definition
```c
BlockNumber RelationGetNumberOfBlocksInFork(Relation relation, ForkNumber forkNum)
```

## Detailed Description
RelationGetNumberOfBlocksInFork provides a unified interface for determining the size of a relation fork in terms of PostgreSQL blocks, regardless of the underlying access method. The function handles two main categories of relations: (1) Relations with table access methods, where it queries the table AM for the size in bytes and converts to blocks by dividing by BLCKSZ (rounding up to handle partial blocks), and (2) Relations with traditional storage that use the storage manager interface directly via smgrnblocks. This abstraction allows PostgreSQL to support both built-in access methods that use fixed-size blocks and external access methods that may use different block sizes or storage formats. The accuracy of results depends on the specific access method implementation.

## Parameters / Member Variables
- `relation`: The relation whose fork size is to be determined
- `forkNum`: The specific fork number (main, FSM, VM, etc.) to measure

## Dependencies
- Functions called/Symbols referenced:
  - RELKIND_HAS_TABLE_AM
  - [table_relation_size](../t/table_relation_size.md)
  - RELKIND_HAS_STORAGE
  - [smgrnblocks](../s/smgrnblocks.md)
  - [RelationGetSmgr](RelationGetSmgr.md)
- Called from (representative examples):
  - [_hash_getnewbuf](../h/_hash_getnewbuf.md)
  - [_hash_init](../h/_hash_init.md)
  - RelationGetNumberOfBlocks (macro wrapper)

## Notes and Other Information
- Handles both table AM relations and traditional storage relations
- Converts byte sizes to block counts for table AM relations by rounding up
- Built-in access methods provide accurate results, external AMs may vary
- Used by RelationGetNumberOfBlocks macro for the main fork
- Critical for determining relation size and managing storage allocation
- Supports all fork types (main, FSM, VM, etc.) through the forkNum parameter