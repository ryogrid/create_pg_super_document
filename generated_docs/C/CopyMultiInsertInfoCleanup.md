# CopyMultiInsertInfoCleanup

## Location
src/backend/commands/copyfrom.c: 567 - 585

## Overview
Performs final cleanup of a CopyMultiInsertInfo structure by cleaning up all remaining buffers and freeing the buffer list.

## Definition
```c
static inline void CopyMultiInsertInfoCleanup(CopyMultiInsertInfo *miinfo)
```

## Detailed Description
This function provides comprehensive cleanup for a CopyMultiInsertInfo structure at the end of a COPY operation. It systematically cleans up all resources by:

1. **Buffer Cleanup**: Iterating through all CopyMultiInsertBuffers in the multiInsertBuffers list and calling CopyMultiInsertBufferCleanup for each one to properly deallocate tuple slots, bulk insert states, and buffer structures.

2. **List Cleanup**: Freeing the multiInsertBuffers list itself using list_free to deallocate the list structure.

This function should be called after all COPY operations are complete to ensure no memory leaks occur. It assumes that buffers have already been flushed if necessary, as CopyMultiInsertBufferCleanup will assert that buffers are empty before cleanup.

## Parameters / Member Variables
- `miinfo`: Pointer to CopyMultiInsertInfo structure to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - CopyMultiInsertBufferCleanup (cleanup of individual buffers)
  - list_free (deallocation of the buffer list)
- Called from (representative examples):
  - CopyFrom (at src/backend/commands/copyfrom.c:1339)

## Notes and Other Information
This function serves as the final cleanup step in the COPY FROM operation lifecycle. It's typically called in error handling paths and at the end of successful COPY operations to ensure proper resource deallocation. The function is designed to be safe to call even if some buffers are empty, as the underlying cleanup functions handle this gracefully.