# index_close

## Location
src/backend/access/index/indexam.c: 177 - 196

## Overview
Closes an index relation and optionally releases the specified lock held on that index.

## Definition
```c
void index_close(Relation relation, LOCKMODE lockmode)
```

## Detailed Description
The `index_close` function closes an index relation that was previously opened with `index_open` or `try_index_open`. It performs the actual closure through the relation cache mechanism and optionally releases locks.

If `lockmode` is not `NoLock`, the function releases the specified lock on the index relation. However, it's often sensible to hold a lock beyond `index_close` for transaction consistency; in such cases, the lock will be automatically released at transaction end.

The function first extracts the lock relation ID from the relation structure, then delegates the actual relation closure to `RelationClose`, and finally handles lock release if requested.

## Parameters
- `relation`: The index relation to close (must be a valid Relation pointer)
- `lockmode`: The type of lock to release (use `NoLock` to keep the lock held)

## Dependencies
- Functions called/Symbols referenced:
  - LockRelId (type)
  - MAX_LOCKMODES (constant)
  - RelationClose
  - UnlockRelationId
- Called from (representative examples):
  - toast_close_indexes
  - systable_endscan
  - index_create
  - ExecCloseIndices
  - get_relation_info

## Notes and Other Information
- The relcache (`RelationClose`) does the real work of closing the relation
- Lock release is optional and controlled by the `lockmode` parameter
- It's common to hold locks beyond the close operation for transactional consistency
- Includes an assertion to validate that lockmode is within valid bounds
- Should be paired with corresponding `index_open` or `try_index_open` calls
- Located in src/backend/access/index/indexam.c:177-196