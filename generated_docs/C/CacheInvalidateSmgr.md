# CacheInvalidateSmgr

## Location
src/backend/utils/cache/inval.c: 1462 - 1491

## Overview
Registers invalidation of storage manager (smgr) references to a physical relation, forcing other backends to close open smgr entries for the relation.

## Definition
```c
void CacheInvalidateSmgr(RelFileLocatorBackend rlocator)
```

## Detailed Description
CacheInvalidateSmgr sends an invalidation message that forces other PostgreSQL backends to close any open storage manager entries for a specified physical relation. This function is used to flush dangling open-file references when the physical relation is being dropped or truncated.

Key characteristics:
- **Nontransactional**: The invalidation is sent immediately without queuing, as it relates to non-rollback-able operations like file drops or truncations
- **WAL independence**: These messages are not captured in commit/abort WAL entries since they're nontransactional
- **Separate from relcache**: Works independently of relcache invalidation since backends may have open smgr entries without relcache entries (e.g., when only writing dirty shared buffers)
- **Space optimization**: Uses only 3 bytes for ProcNumber storage by utilizing padding space in SharedInvalidationMessage

The function constructs a SharedInvalidationMessage with the SHAREDINVALSMGR_ID identifier and the relation's file locator information, then sends it immediately to all backends.

## Parameters / Member Variables
- `rlocator`: RelFileLocatorBackend structure containing the file locator information for the relation to invalidate, including backend ID and physical file location details

## Dependencies
- Functions called/Symbols referenced:
  - RelFileLocatorBackend (data structure)
  - SharedInvalidationMessage (message structure)
  - SHAREDINVALSMGR_ID (message type identifier)
  - VALGRIND_MAKE_MEM_DEFINED (memory debugging macro)
  - SendSharedInvalidMessages (message transmission function)
- Called from (representative examples):
  - vm_extend (src/backend/access/heap/visibilitymap.c:629)
  - smgrdounlinkall (src/backend/storage/smgr/smgr.c:503)
  - smgrtruncate2 (src/backend/storage/smgr/smgr.c:748)

## Notes and Other Information
- The function is designed to handle the maximum ProcNumber of 2^23-1 due to the 3-byte storage limitation
- Should be called from low-level smgr.c routines that execute during both WAL creation and replay
- Essential for maintaining consistency when physical file operations occur that could leave stale file handles in other backends
- The immediate sending (no queuing) ensures that file system operations are properly coordinated across all backends