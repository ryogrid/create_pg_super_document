# _hash_addovflpage

## Location
src/backend/access/hash/hashovfl.c: 112 - 447

## Overview
Adds a new overflow page to a hash bucket chain, handling both recycling of previously freed pages and allocation of new pages when needed.

## Definition
```c
Buffer _hash_addovflpage(Relation rel, Buffer metabuf, Buffer buf, bool retain_pin)
```

## Detailed Description
This function is responsible for extending a hash bucket chain by adding an overflow page. It performs a comprehensive operation that includes:

1. **Finding the tail page**: Traverses the bucket chain to locate the current last page
2. **Searching for free pages**: Scans bitmap pages to find recyclable overflow pages, starting from the hashm_firstfree position
3. **Allocating new pages**: When no free pages are available, extends the relation by allocating new overflow pages and potentially new bitmap pages
4. **Proper locking**: Maintains a strict locking order (tail page → meta page → bitmap page → overflow page) to prevent deadlocks
5. **WAL logging**: Creates a single WAL record covering all changes to ensure atomicity

The function is designed to handle concurrent access safely and includes comprehensive error handling and validation.

## Parameters / Member Variables
- `rel`: The hash index relation being modified
- `metabuf`: Buffer containing the metadata page (caller must hold pin, no lock required)
- `buf`: Buffer pointing to the current last page of the bucket chain (caller must hold pin, no lock required)
- `retain_pin`: Whether to retain the pin on the primary bucket page after completion

## Dependencies
- Functions called/Symbols referenced:
  - LockBuffer/BUFFER_LOCK_EXCLUSIVE/BUFFER_LOCK_UNLOCK (buffer locking)
  - _hash_checkpage (page validation)
  - HashPageGetOpaque/HashPageGetMeta/HashPageGetBitmap (page access)
  - _hash_getbuf/_hash_getinitbuf/_hash_getnewbuf (buffer management)
  - _hash_relbuf (buffer release)
  - bitno_to_blkno (bit number to block number conversion)
  - _hash_firstfreebit (finding first free bit in bitmap)
  - _hash_initbitmapbuffer (initializing new bitmap pages)
  - SETBIT (setting bits in bitmap)
  - XLog functions (WAL logging)
- Called from (representative examples):
  - _hash_doinsert (during tuple insertion when bucket is full)
  - _hash_splitbucket (during bucket splitting operations)
  - HASHNProcs (hash index procedure definitions)

## Notes and Other Information
- The function maintains strict locking order to prevent deadlocks with concurrent operations
- Returns a pinned and write-locked overflow page that is guaranteed to be empty
- Handles bitmap page allocation when the current bitmap pages are exhausted
- Uses a single WAL record for all changes to prevent partial updates in case of crashes
- The retain_pin parameter is typically true only for primary bucket pages
- Includes comprehensive validation and error reporting for bitmap limits
- The function may traverse multiple overflow pages if other processes added pages concurrently