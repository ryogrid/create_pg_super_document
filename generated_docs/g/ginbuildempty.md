# ginbuildempty

## Location
src/backend/access/gin/gininsert.c: 434 - 464

## Overview
The ginbuildempty function creates a minimal empty GIN index structure in the initialization fork, consisting of just the essential meta page and root page.

## Definition


## Detailed Description
The ginbuildempty function creates the most basic valid GIN index structure in the initialization fork. This function is used when creating an empty index that will be populated later, typically during VACUUM FULL or similar operations that rebuild indexes. 

The function performs these key operations:
1. **Page Allocation**: Extends the initialization fork to create space for the meta page and root page
2. **Page Initialization**: Initializes both pages with proper GIN-specific structures
3. **WAL Logging**: Logs both pages to ensure crash recovery consistency
4. **Buffer Management**: Properly releases all acquired buffers

Unlike ginbuild, this function doesn't scan any heap data and creates only the minimal index structure needed for a valid but empty GIN index.

## Parameters / Member Variables
- : The GIN index relation for which to create an empty structure

## Dependencies
- Functions called/Symbols referenced:
  - ExtendBufferedRel
  - BMR_REL 
  - GinInitMetabuffer
  - GinInitBuffer
  - log_newpage_buffer
  - MarkBufferDirty
  - UnlockReleaseBuffer
- Called from (representative examples):
  - ginhandler (via access method handler)

## Notes and Other Information
- Creates exactly two pages: one meta page and one root page (leaf type)
- Uses the initialization fork (INIT_FORKNUM) rather than the main fork
- All operations are performed within a critical section for atomicity
- The root page is initialized as a GIN_LEAF type
- Both pages are immediately logged via log_newpage_buffer for WAL consistency
- Uses EB_LOCK_FIRST and EB_SKIP_EXTENSION_LOCK flags during buffer extension for proper locking
- This function is complementary to ginbuild - while ginbuild creates a full index from heap data, ginbuildempty creates just the skeleton