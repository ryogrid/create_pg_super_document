# BufTableInsert

## Location
[src/backend/storage/buffer/buf_table.c:118-147](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/buf_table.c#L118-L147)

## Overview
Inserts a new hashtable entry mapping a BufferTag to a buffer ID, unless a conflicting entry already exists for that tag.

## Definition
```c
int BufTableInsert(BufferTag *tagPtr, uint32 hashcode, int buf_id)
```

## Detailed Description
BufTableInsert attempts to create a new mapping between a BufferTag and a buffer ID in the shared buffer hashtable. The function uses HASH_ENTER mode to either insert a new entry or find an existing one. If the BufferTag already exists in the hashtable, the function returns the buffer ID of the existing entry, indicating a collision. If the insertion is successful, it returns -1 to signal successful completion. This function is essential for registering newly allocated buffers in the buffer pool and ensuring that buffer mappings are properly maintained.

## Parameters / Member Variables
- `tagPtr`: Pointer to a BufferTag structure that uniquely identifies the buffer (relation, fork, block number)
- `hashcode`: Pre-computed hash value for the BufferTag used for efficient hashtable access
- `buf_id`: The buffer ID to associate with the given BufferTag (must be >= 0)

## Dependencies
- Functions called/Symbols referenced:
  - [hash_search_with_hash_value](../h/hash_search_with_hash_value.md)
  - BufferLookupEnt
  - BufferTag
  - HASH_ENTER
  - P_NEW
- Called from (representative examples):
  - [BufferAlloc](BufferAlloc.md)
  - [ExtendBufferedRelShared](../E/ExtendBufferedRelShared.md)

## Notes and Other Information
The caller must hold an exclusive lock on the BufMappingLock for the tags partition to ensure thread-safe modification of the hashtable. The function includes assertions to validate that the buffer ID is non-negative (since -1 is reserved for "not-in-table") and that the block number is not P_NEW (which represents an invalid tag for extending relations). The return value semantics are important: -1 indicates successful insertion, while any non-negative value indicates the buffer ID of a pre-existing conflicting entry.