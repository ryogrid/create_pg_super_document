# try_index_open

## Location
src/backend/access/index/indexam.c: 152 - 176

## Overview
Opens an index relation by its object identifier (OID) with optional locking, but returns NULL instead of raising an error if the relation does not exist.

## Definition
```c
Relation try_index_open(Oid relationId, LOCKMODE lockmode)
```

## Detailed Description
The `try_index_open` function provides the same functionality as `index_open`, but with graceful error handling. Instead of raising an error when the specified index does not exist, this function returns NULL, allowing the caller to handle the absence of the index appropriately.

Like `index_open`, this function opens an index relation using the provided OID and lock mode, then validates that the opened relation is actually an index. If the relation exists but is not a valid index type, the function will still raise an error through `validate_relation_kind`.

The function is useful in scenarios where the existence of an index is uncertain and the caller prefers to check for NULL rather than handle exceptions.

## Parameters
- `relationId`: The object identifier (OID) of the index relation to open
- `lockmode`: The type of lock to acquire on the index (use `NoLock` if already holding an appropriate lock)

## Dependencies
- Functions called/Symbols referenced:
  - [try_relation_open](try_relation_open.md)
  - [validate_relation_kind](../v/validate_relation_kind.md)
- Called from (representative examples):
  - [reindex_index](../r/reindex_index.md)
  - IndexScanIsValid

## Notes and Other Information
- This is the non-throwing variant of `index_open`
- Returns NULL if the index does not exist, rather than raising an error
- Still validates that the relation is an index if it does exist
- Useful for conditional index operations where the index may or may not exist
- Located in src/backend/access/index/indexam.c:152-176