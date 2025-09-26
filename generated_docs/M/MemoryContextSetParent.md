# MemoryContextSetParent

## Location
[src/backend/utils/mmgr/mcxt.c:637-693](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/mcxt.c#L637-L693)

## Overview
Changes a memory context to belong to a new parent context (or no parent), allowing modification of a context's lifespan after creation.

## Definition

```c
void
MemoryContextSetParent(MemoryContext context, MemoryContext new_parent)
```
## Detailed Description
This function provides the ability to reparent a memory context, which is useful for scenarios where a context's lifespan needs to be modified after creation. A common use case is creating a context under a transient parent, filling it with data, and then moving it under a long-lived parent like CacheMemoryContext to make it persistent.

The function performs the reparenting operation by:
1. Validating that the context and new parent are different to prevent loops
2. Checking if the context already has the correct parent (fast path)
3. Removing the context from its current parent's child list using doubly-linked list operations
4. Adding the context to the new parent's child list as the first child
5. Handling the case where new_parent is NULL (making it a top-level context)

The function is designed to not fail under normal circumstances and avoids elog(ERROR) calls to meet caller expectations. It only checks for direct parent-child loops but doesn't validate against multi-level loops for performance reasons.

## Parameters / Member Variables
- : The memory context to reparent
- : The new parent context (or NULL for no parent)

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextIsValid (validation function)
- Called from (representative examples):
  - RelationBuildRowSecurity
  - SPI_keepplan
  - _SPI_save_plan
  - RelationBuildPartitionDesc
  - UploadManifest
  - exec_parse_message
  - TransferExpandedObject
  - RE_compile_and_cache
  - CompleteCachedPlan
  - SaveCachedPlan
  - CachedPlanSetParentContext

## Notes and Other Information
- Designed to not fail under normal operation - no elog(ERROR) calls
- Only prevents direct parent-child loops, not multi-level cycles
- Uses doubly-linked list operations to maintain parent-child relationships
- Common pattern: create under transient context, then reparent to long-lived context
- Fast path optimization when context already has the correct parent
- Located in src/backend/utils/mmgr/mcxt.c:637-693