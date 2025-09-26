# computeDelta

## Location
src/backend/access/transam/generic_xlog.c: 228 - 268

## Overview
Computes the XLOG delta record needed to transform a current page into a target page and stores it in the PageData structure's delta field.

## Definition


## Detailed Description
This function serves as the high-level coordinator for generating WAL delta records that capture the differences between two PostgreSQL pages. It leverages the page structure's organization, specifically the lower and upper regions defined by the PageHeader, to efficiently compute deltas for only the meaningful portions of the page.

PostgreSQL pages have a specific layout with a lower region (containing line pointers and other metadata) and an upper region (containing tuple data), with free space in between. This function intelligently computes deltas for only these two active regions, ignoring the free space in the middle, which significantly reduces the amount of data that needs to be logged.

The function also includes debugging capability when WAL_DEBUG is enabled, allowing verification that the computed delta can correctly transform the current page into the target page.

## Parameters / Member Variables
- : Pointer to PageData structure where the computed delta will be stored
- : Page pointer to the current state of the page (source)
- : Page pointer to the desired final state of the page (target)

## Dependencies
- Functions called/Symbols referenced:
  - PageData (struct type)
  - PageHeader (struct type for accessing page header fields)
  - computeRegionDelta (called twice - for lower and upper regions at lines 238 and 242)
  - Page (typedef for page pointer)
  - BLCKSZ (constant for block size)
  - PGAlignedBlock (struct for aligned memory blocks, debug only)
  - applyPageRedo (function to apply delta, debug only)
  - memcpy, memcmp (standard library functions, debug only)
  - elog (PostgreSQL logging function, debug only)
- Called from (representative examples):
  - GenericXLogFinish (at line 370)

## Notes and Other Information
- This is a static function, only accessible within generic_xlog.c
- Resets deltaLen to 0 before computing, ensuring a clean delta buffer
- Leverages PostgreSQL's page layout by processing only the lower region (0 to pd_lower) and upper region (pd_upper to BLCKSZ)
- Includes comprehensive debugging support when WAL_DEBUG is enabled to verify delta correctness
- The two-region approach is an optimization that avoids logging the free space between pd_lower and pd_upper
- Part of PostgreSQL's generic WAL logging system for custom access methods
- The debug verification creates a temporary copy of the current page, applies the computed delta, and verifies the result matches the target page