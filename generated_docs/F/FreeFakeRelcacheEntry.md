# FreeFakeRelcacheEntry

## Location
src/backend/access/transam/xlogutils.c: 629 - 640

## Overview
Frees the memory allocated for a fake relation cache entry created by CreateFakeRelcacheEntry().

## Definition
```c
void FreeFakeRelcacheEntry(Relation fakerel)
```

## Detailed Description
This function is a simple cleanup utility that frees the memory allocated for a fake relation cache entry. It is the counterpart to CreateFakeRelcacheEntry() and must be called to prevent memory leaks when the fake relation cache entry is no longer needed.

The function simply calls pfree() on the fake relation pointer, which deallocates the entire structure that was allocated as a single block in CreateFakeRelcacheEntry().

## Parameters / Member Variables
- `fakerel`: Pointer to the fake Relation structure that was created by CreateFakeRelcacheEntry()

## Dependencies
- Functions called/Symbols referenced:
  - pfree

- Called from (representative examples):
  - heap_xlog_visible
  - heap_xlog_delete
  - heap_xlog_insert
  - heap_xlog_multi_insert
  - heap_xlog_update
  - heap_xlog_lock
  - heap_xlog_lock_updated
  - smgrDoPendingSyncs
  - smgr_redo

## Notes and Other Information
- This function must be called for every fake relation cache entry created with CreateFakeRelcacheEntry() to prevent memory leaks
- The function assumes the fake relation was allocated as a single block, which is how CreateFakeRelcacheEntry() allocates it
- Should only be called on relations created by CreateFakeRelcacheEntry(), not on regular relation cache entries
- The function is straightforward and performs no validation or cleanup beyond freeing the memory