# smgrnblocks

## Location
[src/backend/storage/smgr/smgr.c:655-678](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/smgr.c#L655-L678)

## Overview
Calculates and returns the number of blocks in the supplied storage manager relation, utilizing caching to optimize repeated queries for the same relation.

## Definition

```c
BlockNumber
smgrnblocks(SMgrRelation reln, ForkNumber forknum)
```
## Detailed Description
The  function determines the total number of blocks in a specified fork of a storage manager relation. It implements a two-tier approach for efficiency: first checking if a cached value exists using , and if not found, delegating to the storage manager's implementation through the  dispatch table. The result is cached in the relation structure () to avoid repeated expensive disk operations for subsequent queries to the same relation fork.

## Parameters / Member Variables
- : SMgrRelation pointer representing the storage manager relation
- : ForkNumber indicating which fork of the relation to query

## Dependencies
- Functions called/Symbols referenced:
  - SMgrRelation (type)
  - [smgrnblocks_cached](smgrnblocks_cached.md)
  - smgrsw (storage manager dispatch table)
  - InvalidBlockNumber
- Called from (representative examples):
  - [gistBuildCallback](../g/gistBuildCallback.md)
  - [visibilitymap_prepare_truncate](../v/visibilitymap_prepare_truncate.md)
  - [vm_readbuf](../v/vm_readbuf.md)
  - [table_block_relation_size](../t/table_block_relation_size.md)
  - [XLogPrefetcherNextBlock](../X/XLogPrefetcherNextBlock.md)
  - [XLogReadBufferExtended](../X/XLogReadBufferExtended.md)
  - [RelationTruncate](../R/RelationTruncate.md)
  - [RelationCopyStorage](../R/RelationCopyStorage.md)
  - [ExtendBufferedRelTo](../E/ExtendBufferedRelTo.md)
  - [RelationGetNumberOfBlocksInFork](../R/RelationGetNumberOfBlocksInFork.md)
  - [smgrtruncate](smgrtruncate.md)

## Notes and Other Information
- The function uses a caching mechanism to avoid repeated system calls for the same relation
- Returns InvalidBlockNumber if the relation doesn't exist or an error occurs
- Widely used throughout PostgreSQL for buffer management, relation operations, and storage management
- The cached value is stored in  for future use
- Located in src/backend/storage/smgr/smgr.c:655-678