# BufTableDelete

## Location
src/backend/storage/buffer/buf_table.c: 148 - 161

## Overview
BufTableDelete removes an entry from the shared buffer hash table for a given buffer tag, used during buffer invalidation and cleanup operations.

## Definition


## Detailed Description
BufTableDelete is a critical function in PostgreSQL's buffer management system that removes a specific entry from the shared buffer hash table (SharedBufHash). This function is typically called when a buffer is being invalidated or when cleaning up buffer mappings. The function uses the provided hash code for efficient lookup and removal of the hash table entry.

The function performs a hash table search with the HASH_REMOVE operation to delete the entry. If the entry is not found (which should not happen under normal circumstances), it raises an ERROR indicating corruption in the shared buffer hash table.

The caller must hold an exclusive lock on BufMappingLock for the tag's partition before calling this function to ensure thread safety and data consistency.

## Parameters / Member Variables
- : Pointer to a BufferTag structure that uniquely identifies the buffer page to be removed from the hash table
- : Pre-computed hash value for the buffer tag, used for efficient hash table operations

## Dependencies
- Functions called/Symbols referenced:
  - hash_search_with_hash_value
  - HASH_REMOVE
  - BufferLookupEnt
  - BufferTag
- Called from (representative examples):
  - InvalidateBuffer (src/backend/storage/buffer/bufmgr.c:1847)
  - InvalidateVictimBuffer (src/backend/storage/buffer/bufmgr.c:1926)
  - ResourceOwnerForgetBufferIO (src/include/storage/buf_internals.h:443)

## Notes and Other Information
- The caller must hold exclusive lock on BufMappingLock for the tag's partition before calling this function
- If the hash table entry is not found, the function will raise an ERROR with the message "shared buffer hash table corrupted"
- This function is part of the buffer management subsystem and is critical for maintaining consistency in the shared buffer pool
- The function is located in src/backend/storage/buffer/buf_table.c at lines 148-161