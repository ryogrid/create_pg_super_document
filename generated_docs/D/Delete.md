# Delete

## Location
src/backend/storage/file/fd.c: 1265 - 1283

## Overview
Delete is a static function that removes a virtual file descriptor from the LRU (Least Recently Used) doubly-linked list in PostgreSQL's VFD cache management system.

## Definition

```c
static void
Delete(File file)
```
## Detailed Description
This function removes a specified virtual file descriptor from the LRU chain by updating the doubly-linked list pointers to bypass the target VFD. It does not close or deallocate the file descriptor itself, but simply removes it from the LRU ordering mechanism used for cache management.

The function works by accessing the VFD at the specified file index and updating the lruLessRecently and lruMoreRecently pointers of the adjacent VFDs to point to each other, effectively removing the target VFD from the chain. Debug logging shows the LRU state before and after the deletion when FDDEBUG is enabled.

This function is a fundamental building block for VFD cache management, used when files need to be removed from LRU tracking without necessarily closing them.

## Parameters / Member Variables
- : The File index (VFD index) to remove from the LRU chain

## Dependencies
- Functions called/Symbols referenced:
  - File (typedef for VFD index)
  - Vfd (VFD structure type)
  - VfdCache (global VFD cache array)
  - DO_DB (debug macro)
  - _dump_lru (debugging function)
  - elog (logging function)
  - Assert (assertion macro)
- Called from (representative examples):
  - AllocateDesc
  - LruDelete
  - FileAccess
  - FileClose

## Notes and Other Information
- Static function, only accessible within the fd.c source file
- Includes assertion to prevent deletion of VFD index 0 (invalid/sentinel value)
- Uses DO_DB macro for conditional debug logging when FDDEBUG is enabled
- Does not actually close the file or free resources, only removes from LRU chain
- Part of the fundamental VFD cache management infrastructure
- Debug output shows the file name and index being deleted when enabled