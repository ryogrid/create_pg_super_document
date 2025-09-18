# RelationClose

## Location
src/backend/utils/cache/relcache.c: 2194 - 2202

## Overview
Closes an open relation by decrementing its reference count and performing necessary cleanup operations.

## Definition
```c
void RelationClose(Relation relation)
```

## Detailed Description
RelationClose serves as the primary interface for closing relation references in PostgreSQL. Despite its name suggesting it "closes" a relation, it actually decrements the relation's reference count and delegates to RelationCloseCleanup for additional cleanup tasks.

The function is designed to be simple and efficient, requiring no locking operations. When compiled with the RELCACHE_FORCE_RELEASE debug option, relation cache entries are immediately freed when their reference count reaches zero, which helps catch dangling pointer bugs during development.

The two-step process ensures proper cleanup:
1. Decrements the reference count through RelationDecrementReferenceCount
2. Performs additional cleanup operations through RelationCloseCleanup

## Parameters / Member Variables
- `relation`: Pointer to the Relation structure to be closed

## Dependencies
- Functions called/Symbols referenced:
  - RelationDecrementReferenceCount
  - RelationCloseCleanup
- Called from (representative examples):
  - relation_close
  - index_close
  - ReorderBufferProcessTXN
  - pgoutput_change

## Notes and Other Information
- No locking operations are required as the function operates on reference counting mechanisms
- The RELCACHE_FORCE_RELEASE compile-time option enables aggressive cleanup for debugging purposes, making it easier to detect use-after-free bugs
- This is the standard way to release relation references throughout the PostgreSQL codebase
- The actual freeing of relation cache entries depends on the reference count reaching zero and cache pressure
- Always use this function instead of directly manipulating reference counts to ensure proper cleanup