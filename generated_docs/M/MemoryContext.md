# MemoryContext

## Location
src/include/utils/palloc.h: 36 - 46

## Overview
A pointer typedef to MemoryContextData structure that represents PostgreSQL's memory allocation contexts, providing hierarchical memory management with automatic cleanup.

## Definition
```c
typedef struct MemoryContextData *MemoryContext;
```

## Detailed Description
MemoryContext is a fundamental type in PostgreSQL's memory management system, implemented as a pointer to MemoryContextData structure. It provides a hierarchical memory management framework where memory allocations can be grouped into contexts that can be efficiently reset or deleted together. The context system allows PostgreSQL to manage memory in logical groups, making it easy to clean up all memory associated with a particular operation or subsystem. This design helps prevent memory leaks and provides efficient bulk memory management. The actual implementation details are contained in the MemoryContextData structure defined in nodes/memnodes.h.

## Parameters / Member Variables
The MemoryContextData structure contains:
- `type`: NodeTag identifying the exact kind of context
- `isReset`: Boolean flag indicating if no space has been allocated since last reset
- `allowInCritSection`: Boolean allowing palloc in critical sections
- `mem_allocated`: Size tracking memory allocated for this context
- `methods`: Pointer to virtual function table for context operations
- `parent`: Parent context (NULL for toplevel contexts)
- `firstchild`: Head of linked list of child contexts
- `prevchild`: Previous sibling context
- `nextchild`: Next sibling context
- `name`: Context name for debugging purposes
- `ident`: Context identifier for debugging purposes
- `reset_cbs`: List of reset/delete callback functions

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextData (underlying structure)
  - MemoryContextMethods (virtual function table)
  - MemoryContextCallback (callback mechanism)
- Called from (representative examples):
  - MemoryContextSwitchTo
  - Memory allocation functions throughout PostgreSQL

## Notes and Other Information
- Defined in src/include/utils/palloc.h:36-46
- Actual structure implementation in src/include/nodes/memnodes.h:117-134
- Provides abstraction layer hiding implementation details from most users
- Central to PostgreSQL's memory management architecture
- Supports hierarchical organization with parent-child relationships
- Includes callback mechanism for cleanup operations
- Used extensively throughout PostgreSQL for memory organization