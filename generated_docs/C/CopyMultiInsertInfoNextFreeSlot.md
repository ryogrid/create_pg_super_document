# CopyMultiInsertInfoNextFreeSlot

## Location
src/backend/commands/copyfrom.c: 586 - 604

## Overview
Returns the next available TupleTableSlot from a result relation's multi-insert buffer for storing the next tuple during COPY operations.

## Definition
```c
static inline TupleTableSlot *
CopyMultiInsertInfoNextFreeSlot(CopyMultiInsertInfo *miinfo,
                               ResultRelInfo *rri)
```

## Detailed Description
This function provides access to the next free slot in a CopyMultiInsertBuffer associated with a specific result relation. It is used during COPY FROM operations when buffering multiple tuples before performing batch inserts for performance optimization. The function ensures that tuple slots are lazily allocated only when needed - if a slot at the current position doesn't exist, it creates a new one using the relation's tuple descriptor.

The function operates on the assumption that the caller has already verified the buffer is not full, as indicated by the assertion that checks nused < MAX_BUFFERED_TUPLES.

## Parameters / Member Variables
- `miinfo`: CopyMultiInsertInfo pointer (unused parameter, kept for API consistency)
- `rri`: ResultRelInfo pointer containing the relation and its associated multi-insert buffer

## Dependencies
- Functions called/Symbols referenced:
  - table_slot_create
  - CopyMultiInsertBuffer (struct)
  - CopyMultiInsertInfo (struct)  
  - MAX_BUFFERED_TUPLES (constant)
- Called from (representative examples):
  - CopyFrom (at lines 979, 1140)

## Notes and Other Information
- This is a static inline function for performance, as it's called frequently during COPY operations
- The miinfo parameter is explicitly noted as unused but maintained for API consistency
- Callers must ensure the buffer is not full before calling this function
- Tuple slots are created lazily to avoid unnecessary memory allocation
- Part of the multi-insert optimization mechanism that batches tuple insertions for better performance