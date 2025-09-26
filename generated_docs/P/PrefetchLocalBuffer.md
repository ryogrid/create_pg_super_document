# PrefetchLocalBuffer

## Location
[src/backend/storage/buffer/localbuf.c:69-115](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/localbuf.c#L69-L115)

## Overview
PrefetchLocalBuffer initiates asynchronous read operations for blocks of temporary relations, serving as the local buffer equivalent of PrefetchBuffer for non-shared temporary relations.

## Definition

```c
PrefetchBufferResult
PrefetchLocalBuffer(SMgrRelation smgr, ForkNumber forkNum,
					BlockNumber blockNum)
```
## Detailed Description
PrefetchLocalBuffer handles prefetching for temporary relations that use local buffers rather than shared buffers. The function first checks if the requested block already exists in the local buffer hash table. If the block is already present, no I/O operation is needed. If the block is not found and prefetching is enabled (USE_PREFETCH), it initiates an asynchronous read operation through the storage manager, but only when direct I/O is not being used for data files.

The function follows a simple but effective strategy: avoid unnecessary I/O for blocks already in memory while enabling performance optimization through prefetching for blocks that will need to be loaded.

## Parameters
- : Storage manager relation handle for the temporary relation
- : Fork number specifying which fork of the relation (main, FSM, visibility map, etc.)
- : Block number within the specified fork to prefetch

## Dependencies
- Functions called/Symbols referenced:
  - InitBufferTag: Creates buffer tag for the requested block
  - InitLocalBuffers: Initializes local buffer system if not already done
  - hash_search: Searches local buffer hash table for existing block
  - smgrprefetch: Initiates actual prefetch I/O operation through storage manager
- Called from (representative examples):
  - PrefetchBuffer: Main prefetch function delegates to this for temporary relations
  - ResourceOwnerForgetBufferIO: Buffer resource management

## Notes and Other Information
- Only functional when USE_PREFETCH is compiled in, otherwise it's effectively a no-op
- Skips prefetching when direct I/O is enabled for data files (IO_DIRECT_DATA flag)
- Returns a PrefetchBufferResult structure indicating whether I/O was initiated and any relevant buffer information
- Part of PostgreSQL's local buffer management system specifically designed for temporary relations
- The negative buffer ID encoding (-hresult->id - 1) follows PostgreSQL's convention for local buffer identification