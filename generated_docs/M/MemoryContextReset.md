# MemoryContextReset

## Location
src/backend/utils/mmgr/mcxt.c: 383 - 401

## Overview
MemoryContextReset releases all allocated space within a memory context and deletes all its descendant contexts while preserving the context itself for future use.

## Definition
```c
void MemoryContextReset(MemoryContext context)
```

## Detailed Description
This function provides a comprehensive reset mechanism for memory contexts that combines child context deletion with memory cleanup. It performs two main operations:

1. **Child Context Deletion**: If the context has child contexts (context->firstchild != NULL), it calls MemoryContextDeleteChildren to recursively delete all descendant contexts
2. **Memory Reset**: If the context has been used for allocations since startup or the last reset (!context->isReset), it calls MemoryContextResetOnly to free all allocated memory within the context

The function includes performance optimizations by checking conditions before making function calls, avoiding unnecessary work when there are no children or when the context is already in a reset state. After completion, the context is ready for immediate reuse with a clean slate.

## Parameters / Member Variables
- `context`: The MemoryContext to reset - must be a valid memory context

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextIsValid (to validate the context parameter)
  - [MemoryContextDeleteChildren](MemoryContextDeleteChildren.md) (to remove all child contexts)
  - [MemoryContextResetOnly](MemoryContextResetOnly.md) (to free allocated memory within the context)
- Called from (representative examples):
  - [brininsert](../b/brininsert.md), bringetbitmap (BRIN index operations)
  - [ginInsertCleanup](../g/ginInsertCleanup.md), ginBuildCallback (GIN index operations)
  - [gistinsert](../g/gistinsert.md), gistBuildCallback (GiST index operations)
  - [ExecHashTableReset](../E/ExecHashTableReset.md), ExecProjectSet (query execution)
  - [do_autovacuum](../d/do_autovacuum.md), perform_work_item (autovacuum operations)
  - [PostgresMain](../P/PostgresMain.md) (main query processing loop)
  - Many other locations throughout PostgreSQL for periodic memory cleanup

## Notes and Other Information
- This is one of the most frequently used memory context functions in PostgreSQL
- Provides a clean slate for contexts that will be reused rather than deleted
- The function is performance-optimized with condition checks to avoid unnecessary work
- Essential for preventing memory bloat in long-running operations that repeatedly use the same context
- Unlike MemoryContextDelete, this preserves the context structure for continued use
- Commonly used in query execution to reset contexts between processing cycles
- The context->isReset flag optimization prevents redundant reset operations
- Safe to call multiple times on the same context without side effects