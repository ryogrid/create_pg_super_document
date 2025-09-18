# BufTableLookup

## Location
src/backend/storage/buffer/buf_table.c: 90 - 117

## Overview
Searches the shared buffer hashtable for a given BufferTag and returns the corresponding buffer ID, or -1 if the buffer is not found.

## Definition
```c
int BufTableLookup(BufferTag *tagPtr, uint32 hashcode)
```

## Detailed Description
BufTableLookup performs a lookup operation in the shared buffer hashtable to find a buffer associated with a specific BufferTag. The function uses the pre-computed hash code to efficiently locate the correct hash bucket and searches for a matching entry. If found, it returns the buffer ID from the BufferLookupEnt structure; otherwise, it returns -1 to indicate that the requested buffer is not currently in the buffer pool. This function is a critical component of buffer management, allowing the system to quickly determine whether a particular page is already cached in memory.

## Parameters / Member Variables
- `tagPtr`: Pointer to a BufferTag structure that uniquely identifies the buffer (relation, fork, block number)
- `hashcode`: Pre-computed hash value for the BufferTag (obtained via BufTableHashCode) used for efficient hashtable access

## Dependencies
- Functions called/Symbols referenced:
  - [hash_search_with_hash_value](../h/hash_search_with_hash_value.md)
  - BufferLookupEnt
  - BufferTag
  - HASH_FIND
- Called from (representative examples):
  - [PrefetchSharedBuffer](../P/PrefetchSharedBuffer.md)
  - [BufferAlloc](BufferAlloc.md)
  - [FindAndDropRelationBuffers](../F/FindAndDropRelationBuffers.md)

## Notes and Other Information
The caller must hold at least a share lock on the BufMappingLock for the tags partition before calling this function to ensure thread-safe access to the hashtable. The function uses HASH_FIND mode to perform a read-only lookup without modifying the hashtable structure. The hash code parameter avoids redundant hash computation since the same hash value is typically needed for partition lock selection.