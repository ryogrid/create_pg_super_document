# MemoryContextCreate

## Location
[src/backend/utils/mmgr/mcxt.c:1100-1146](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/mcxt.c#L1100-L1146)

## Overview
MemoryContextCreate is the context-type-independent core function for creating memory contexts in PostgreSQL, responsible for initializing the generic header fields and linking new contexts into the context tree.

## Definition

```c
void
MemoryContextCreate(MemoryContext node,
					NodeTag tag,
					MemoryContextMethodID method_id,
					MemoryContext parent,
					const char *name)
```
## Detailed Description
This function serves as the foundation for all memory context creation in PostgreSQL. It's designed to be called only by context-type-specific creation routines (like AllocSetContextCreate, BumpContextCreate, etc.), not directly by general application code.

The function follows a carefully orchestrated creation procedure:
1. Context-specific routines first allocate initial space including the context header
2. They set up type-specific fields and management structures
3. MemoryContextCreate is called to initialize generic header fields and link the context
4. Context-specific routines complete any remaining initialization

The function initializes all standard fields of the memory context header, establishes parent-child relationships in the context tree, and ensures proper inheritance of critical section permissions. It also integrates with Valgrind for memory debugging support.

## Parameters / Member Variables
- `node`: The uninitialized common part of the context header node to be initialized
- `tag`: NodeTag code that identifies the specific memory context type being created
- `method_id`: MemoryContextMethodID specifying which context implementation methods to use
- `parent`: Parent memory context, or NULL if creating a top-level context
- `*name`: Human-readable name for the context (must be statically allocated string)
## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextMethodID (for method dispatch table access)
  - VALGRIND_CREATE_MEMPOOL (for memory debugging integration)
- Called from (representative examples):
  - [AllocSetContextCreateInternal](../A/AllocSetContextCreateInternal.md)
  - [BumpContextCreate](../B/BumpContextCreate.md)
  - [GenerationContextCreate](../G/GenerationContextCreate.md)
  - [SlabContextCreate](../S/SlabContextCreate.md)

## Notes and Other Information
- This function assumes it cannot fail and uses Assert rather than elog/ereport for error conditions
- Creating new memory contexts is prohibited in critical sections (Assert(CritSectionCount == 0))
- The allowInCritSection flag is inherited from the parent context, defaulting to false for top-level contexts
- The function establishes bidirectional parent-child links for efficient context tree traversal
- Context names must be statically allocated strings to avoid memory management complications
- Integration with Valgrind mempool tracking aids in debugging memory-related issues

## Simplified Source

```c
void
MemoryContextCreate(MemoryContext node,
                    NodeTag tag,
                    MemoryContextMethodID method_id,
                    MemoryContext parent,
                    const char *name)
{
    // Must not create contexts in critical sections
    Assert(CritSectionCount == 0);

    // Initialize all standard header fields
    node->type = tag;
    node->isReset = true;
    node->methods = &mcxt_methods[method_id];
    node->parent = parent;
    node->firstchild = NULL;
    node->mem_allocated = 0;
    node->prevchild = NULL;
    node->name = name;
    node->ident = NULL;
    node->reset_cbs = NULL;

    // Link into context tree structure
    if (parent) {
        // Insert as first child of parent
        node->nextchild = parent->firstchild;
        if (parent->firstchild != NULL)
            parent->firstchild->prevchild = node;
        parent->firstchild = node;

        // Inherit critical section permission from parent
        node->allowInCritSection = parent->allowInCritSection;
    } else {
        // Top-level context
        node->nextchild = NULL;
        node->allowInCritSection = false;
    }

    // Register with Valgrind for memory debugging
    VALGRIND_CREATE_MEMPOOL(node, 0, false);
}
```