# DropRelationAllLocalBuffers

## Location
[src/backend/storage/buffer/localbuf.c:537-579](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/localbuf.c#L537-L579)

## Overview
Removes all pages of all forks of a specified relation from the local buffer pool without writing dirty pages to disk first.

## Definition
void DropRelationAllLocalBuffers(RelFileLocator rlocator)

## Detailed Description
This function removes all pages belonging to a specified relation from the local buffer pool, regardless of fork number or block number. It's typically used when dropping temporary tables and indexes entirely. Like DropRelationLocalBuffers, this function is dangerous because it discards dirty pages without writing them to disk first, making it non-rollback-able.

The function iterates through all local buffers and removes any that belong to the specified relation, regardless of which fork (main, FSM, VM, etc.) they belong to. It performs safety checks to ensure no buffers are still referenced and removes them from both the buffer descriptors and the local buffer hash table.

## Parameters / Member Variables
- `rlocator`: RelFileLocator identifying the relation whose buffers should be dropped from all forks

## Dependencies
- Functions called/Symbols referenced:
  - [GetLocalBufferDescriptor](../G/GetLocalBufferDescriptor.md)
  - [pg_atomic_read_u32](../p/pg_atomic_read_u32.md)
  - [BufTagMatchesRelFileLocator](../B/BufTagMatchesRelFileLocator.md)
  - [BufTagGetRelFileLocator](../B/BufTagGetRelFileLocator.md)
  - [BufTagGetForkNum](../B/BufTagGetForkNum.md)
  - relpathbackend
  - [hash_search](../h/hash_search.md)
  - [ClearBufferTag](../C/ClearBufferTag.md)
  - [pg_atomic_unlocked_write_u32](../p/pg_atomic_unlocked_write_u32.md)
- Called from (representative examples):
  - [DropRelationsAllBuffers](DropRelationsAllBuffers.md)
  - [ResourceOwnerForgetBufferIO](../R/ResourceOwnerForgetBufferIO.md)

## Notes and Other Information
- WARNING: This function is NOT rollback-able - dirty pages are discarded without being written to disk
- Removes buffers from ALL forks of the relation (main, FSM, visibility map, etc.)
- Should only be used with extreme caution, typically when dropping entire temporary relations
- Throws ERROR if any buffer is still referenced, indicating a buffer management bug
- Removes buffers from both the buffer pool and LocalBufHash hash table
- Only operates on local buffers (temporary tables/indexes visible to current session)
- Uses atomic operations to safely read and modify buffer state
- More comprehensive than DropRelationLocalBuffers as it doesn't filter by fork or block range