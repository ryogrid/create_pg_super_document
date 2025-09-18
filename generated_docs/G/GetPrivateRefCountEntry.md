# GetPrivateRefCountEntry

## Location
src/backend/storage/buffer/bufmgr.c: 341 - 414

## Overview
GetPrivateRefCountEntry retrieves the PrivateRefCount entry for a specified buffer, optionally optimizing future access by moving hash table entries to the array.

## Definition
```c
static PrivateRefCountEntry *GetPrivateRefCountEntry(Buffer buffer, bool do_move)
```

## Detailed Description
This function implements a two-tier lookup system for buffer reference count entries, searching first in the fast-access array and then in the hash table if necessary. It's a core component of PostgreSQL's buffer reference counting mechanism.

The function first performs a linear search through PrivateRefCountArray for the target buffer. If not found there, it checks if any entries have previously overflowed to the hash table (PrivateRefCountOverflowed > 0) before performing a hash lookup.

When do_move is true and the entry is found in the hash table, the function optimizes future access by moving the entry back to the array. This involves reserving an array slot, copying the entry data, and removing it from the hash table.

## Parameters / Member Variables
- `buffer`: The Buffer ID to look up in the reference count tracking system
- `do_move`: If true, move hash table entries back to the array for faster future access

## Dependencies
- Functions called/Symbols referenced:
  - BufferIsValid
  - BufferIsLocal
  - hash_search
  - HASH_FIND
  - HASH_REMOVE
  - ReservePrivateRefCountEntry
  - PrivateRefCountEntry (struct type)
  - REFCOUNT_ARRAY_ENTRIES (macro)
- Called from (representative examples):
  - GetPrivateRefCount
  - PinBuffer
  - PinBuffer_Locked
  - UnpinBufferNoOwner
  - IncrBufferRefCount

## Notes and Other Information
- Returns NULL if the buffer doesn't have a refcount entry
- Only works with shared memory buffers (not local buffers)
- The do_move optimization helps maintain array locality for frequently accessed buffers
- Includes assertions to ensure buffer validity and proper local buffer handling
- The hash table lookup is skipped entirely if no overflow has occurred, optimizing the common case
- When moving from hash to array, properly maintains the PrivateRefCountOverflowed counter