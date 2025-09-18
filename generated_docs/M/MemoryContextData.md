# MemoryContextData

## Location
src/include/nodes/memnodes.h: 117 - 134

## Overview
The core data structure that represents a memory context in PostgreSQL, containing metadata, hierarchical relationships, and function pointers that define how memory allocation and management operations are performed.

## Definition
```c
typedef struct MemoryContextData
{
    pg_node_attr(abstract)      /* there are no nodes of this type */
    
    NodeTag     type;           /* identifies exact kind of context */
    /* these two fields are placed here to minimize alignment wastage: */
    bool        isReset;        /* T = no space alloced since last reset */
    bool        allowInCritSection; /* allow palloc in critical section */
    Size        mem_allocated;  /* track memory allocated for this context */
    const MemoryContextMethods *methods;    /* virtual function table */
    MemoryContext parent;       /* NULL if no parent (toplevel context) */
    MemoryContext firstchild;   /* head of linked list of children */
    MemoryContext prevchild;    /* previous child of same parent */
    MemoryContext nextchild;    /* next child of same parent */
    const char *name;           /* context name (just for debugging) */
    const char *ident;          /* context ID if any (just for debugging) */
    MemoryContextCallback *reset_cbs;   /* list of reset/delete callbacks */
} MemoryContextData;
```

## Detailed Description
MemoryContextData is the fundamental structure that implements PostgreSQL's hierarchical memory management system. Each memory context maintains a tree structure where contexts can have parent-child relationships, enabling automatic cleanup when parent contexts are destroyed. The structure combines metadata tracking, operational state, and a virtual function table pattern through the MemoryContextMethods pointer.

The context supports different allocation strategies through its methods pointer, tracks allocation state to optimize operations, and provides safety mechanisms like critical section protection. The hierarchical design allows for scoped memory management where related allocations can be grouped and released together, which is essential for PostgreSQL's transaction and query processing lifecycle.

## Parameters / Member Variables
- `type`: NodeTag that identifies the exact kind of memory context (AllocSet, Generation, Bump, etc.)
- `isReset`: Boolean flag indicating whether any memory has been allocated since the last reset operation
- `allowInCritSection`: Boolean flag controlling whether allocations are permitted during critical sections
- `mem_allocated`: Running total of bytes allocated through this context for tracking purposes
- `methods`: Pointer to the MemoryContextMethods structure containing function pointers for operations
- `parent`: Pointer to parent context in the hierarchy, or NULL for top-level contexts
- `firstchild`: Head pointer for the linked list of child contexts
- `prevchild`: Pointer to the previous sibling context under the same parent
- `nextchild`: Pointer to the next sibling context under the same parent
- `name`: Human-readable name for the context used in debugging and error reporting
- `ident`: Optional additional identifier string for debugging purposes
- `reset_cbs`: Linked list of callback functions to execute during reset or delete operations

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextMethods
  - MemoryContextCallback
  - NodeTag
  - Size
  - pg_node_attr (node annotation)
- Called from (representative examples):
  - AllocSetContext (aset.c:154)
  - BumpContext (bump.c:68)
  - GenerationContext (generation.c:61)
  - SlabContext (slab.c:105)
  - ErrorData (elog.h:472)

## Notes and Other Information
- Marked with pg_node_attr(abstract) indicating it's an abstract base type with no direct instances
- The isReset and allowInCritSection fields are positioned to minimize memory alignment wastage
- Implements a tree structure through parent/child pointers enabling hierarchical memory management
- The MemoryContext typedef is typically used as a pointer to this structure (MemoryContextData *)
- Critical sections are periods where interrupts must not cause longjmp, making normal palloc unsafe
- Used as the base type for all specific memory context implementations in PostgreSQL
- The callback system allows for cleanup operations beyond simple memory deallocation
- Located in src/include/nodes/memnodes.h as part of the core memory management infrastructure