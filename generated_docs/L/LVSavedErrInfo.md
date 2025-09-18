# LVSavedErrInfo

## Location
src/backend/access/heap/vacuumlazy.c: 222 - 227

## Overview
LVSavedErrInfo is a simple structure used to save and restore vacuum error context information during PostgreSQL's lazy vacuum operations.

## Definition
```c
typedef struct LVSavedErrInfo
{
    BlockNumber blkno;
    OffsetNumber offnum;
    VacErrPhase phase;
} LVSavedErrInfo;
```

## Detailed Description
LVSavedErrInfo provides a mechanism for temporarily saving vacuum error reporting context during operations that may change the current error context. This is essential for maintaining accurate error reporting when vacuum operations switch between different phases or operate on different parts of the relation. The structure allows vacuum functions to save their current error state, perform operations that might modify the error context, and then restore the original context afterward.

## Parameters / Member Variables
- `blkno`: Block number being processed when the error context was saved
- `offnum`: Offset number within the block when the error context was saved  
- `phase`: The vacuum phase that was active when the error context was saved

## Dependencies
- Functions called/Symbols referenced:
  - VacErrPhase

- Called from (representative examples):
  - lazy_vacuum_heap_rel
  - lazy_vacuum_heap_page
  - lazy_vacuum_one_index
  - lazy_cleanup_one_index
  - update_vacuum_error_info
  - restore_vacuum_error_info

## Notes and Other Information
This structure is typically used in pairs with update_vacuum_error_info() and restore_vacuum_error_info() functions to temporarily change and then restore vacuum error reporting context. It ensures that if an error occurs during nested vacuum operations, the error message will contain accurate information about where the error occurred rather than potentially stale context from a previous operation.