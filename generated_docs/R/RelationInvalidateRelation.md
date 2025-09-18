# RelationInvalidateRelation

## Location
src/backend/utils/cache/relcache.c: 2522 - 2560

## Overview
RelationInvalidateRelation marks a relation cache entry as invalid, ensuring it will be reloaded on next access while cleaning up file handles and cached access method data.

## Definition
```c
static void RelationInvalidateRelation(Relation relation)
```

## Detailed Description
RelationInvalidateRelation is a lightweight invalidation function that marks a relation cache entry as needing reload without completely destroying it. This function is typically used when the relation's metadata may have changed (such as after a VACUUM truncation) but the relation structure remains fundamentally the same.

The function performs minimal but essential cleanup:
1. Closes storage manager files to ensure file state consistency after operations like vacuum truncation
2. Frees any cached access method data that may be stale
3. Marks the relation as invalid (rd_isvalid = false) to trigger reload on next access

This approach allows for efficient cache invalidation without the overhead of complete entry destruction and recreation.

## Parameters / Member Variables
- `relation`: The Relation structure to invalidate. The entry will be marked invalid and cleaned up for later reload.

## Dependencies
- Functions called/Symbols referenced:
  - RelationCloseSmgr
- Called from (representative examples):
  - RelationFlushRelation

## Notes and Other Information
- Lighter weight alternative to complete relation destruction
- Ensures file handle consistency after storage operations
- Triggers automatic reload on next relation access
- Preserves most cached data while invalidating stale components
- Commonly used after operations that change file-level properties but not relation structure
- Part of PostgreSQL's incremental cache invalidation strategy