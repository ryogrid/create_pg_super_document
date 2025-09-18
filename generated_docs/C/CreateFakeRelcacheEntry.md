# CreateFakeRelcacheEntry

## Location
[src/backend/access/transam/xlogutils.c:582-628](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogutils.c#L582-L628)

## Overview
Creates a fake relation cache entry for physical relations during XLOG replay and WAL-skipped file syncing, enabling the use of standard functions that expect a relcache entry.

## Definition
```c
Relation CreateFakeRelcacheEntry(RelFileLocator rlocator)
```

## Detailed Description
This function creates a minimal relation cache entry that contains only the fields necessary for physical storage operations during XLOG replay or WAL-skipped file syncing. Since PostgreSQL's normal relation cache is not available during WAL replay, this fake entry allows low-level storage functions like ReadBuffer() to operate normally.

The function allocates memory for a fake relation cache entry and initializes key fields including the relation locator, persistence setting, backend information, and storage manager reference. It sets up a bogus lock relation ID for potential locking operations, though conflicts are unlikely during recovery since PostgreSQL runs single-threaded during replay.

## Parameters / Member Variables
- `rlocator`: RelFileLocator specifying the database OID, tablespace OID, and relation number for the target relation

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)
  - sprintf
  - [smgropen](../s/smgropen.md)
  - FakeRelCacheEntry
  - [FakeRelCacheEntryData](../F/FakeRelCacheEntryData.md)
  - INVALID_PROC_NUMBER
  - RELPERSISTENCE_PERMANENT

- Called from (representative examples):
  - [heap_xlog_visible](../h/heap_xlog_visible.md)
  - [heap_xlog_delete](../h/heap_xlog_delete.md)
  - [heap_xlog_insert](../h/heap_xlog_insert.md)
  - [heap_xlog_multi_insert](../h/heap_xlog_multi_insert.md)
  - [heap_xlog_update](../h/heap_xlog_update.md)
  - [heap_xlog_lock](../h/heap_xlog_lock.md)
  - [heap_xlog_lock_updated](../h/heap_xlog_lock_updated.md)
  - [smgrDoPendingSyncs](../s/smgrDoPendingSyncs.md)
  - [smgr_redo](../s/smgr_redo.md)

## Notes and Other Information
- The fake entry must be freed using FreeFakeRelcacheEntry() to prevent memory leaks
- Only fields related to physical storage are initialized, making it unsuitable for high-level operations
- The relation name is set to the relation number since the actual name is unknown during replay
- The lock relation ID setup is somewhat bogus since relNumber may differ from the relation's OID, but this doesn't matter in practice during recovery
- Assumes the relation is permanent (not temporary) since temp relations are not processed during recovery or WAL-skipped file syncing
- Sets up a non-pinned SMgrRelation reference to avoid cleanup complications on errors