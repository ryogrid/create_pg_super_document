# FreePageManagerDump

## Location
src/backend/utils/mmgr/freepage.c: 424 - 500

## Overview
Produces a detailed debugging dump of the internal state of a free page manager for diagnostic purposes.

## Definition


## Detailed Description
This debugging function generates a comprehensive textual representation of the free page manager's internal state. The output includes all major data structures and their current contents, making it invaluable for troubleshooting memory management issues.

The dump includes:
1. **Metadata**: Self-pointer offset and maximum contiguous pages available
2. **B-tree structure**: If a B-tree exists (depth > 0), dumps the entire tree structure via recursive calls to 
3. **Singleton information**: For simple cases where only one free span exists
4. **Recycle list**: B-tree nodes available for reuse
5. **Free lists**: All non-empty freelists showing available page spans

The function constructs the output using PostgreSQL's StringInfo buffer mechanism and returns a dynamically allocated string that the caller must free.

## Parameters / Member Variables
- : Pointer to the FreePageManager structure to dump

## Dependencies
- Functions called/Symbols referenced:
  - fpm_segment_base
  - FreePageSpanLeader (struct type)
  - FreePageBtree (struct type)
  - relptr_access
  - [FreePageManagerDumpBtree](FreePageManagerDumpBtree.md)
  - [FreePageManagerDumpSpans](FreePageManagerDumpSpans.md)
  - FPM_NUM_FREELISTS (constant)
- Called from (representative examples):
  - fpm_largest (likely a debugging/testing function)

## Notes and Other Information
This is a debugging utility function that provides human-readable output for analyzing the internal state of the free page manager. The returned string is dynamically allocated and must be freed by the caller. The function is primarily useful during development, testing, and troubleshooting memory management issues. The output format is designed to be readable and includes hierarchical indentation for complex structures like B-trees.