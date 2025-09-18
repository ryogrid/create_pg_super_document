# gistSplitByKey

## Location
src/backend/access/gist/gistsplit.c: 623 - 779

## Overview
Main entry point for the GiST page-splitting algorithm that recursively processes index columns to optimize splits and handle complex multi-column scenarios.

## Definition


## Detailed Description
This function implements the sophisticated splitting algorithm for GiST index pages, handling multi-column indexes with recursive optimization. The process involves several phases:

1. **Entry Vector Preparation**: Creates a vector of GISTENTRY structures from the input tuples and identifies tuples with null values in the current column.

2. **Null Handling**: Implements a policy of separating null and non-null values, placing nulls on the right side and non-nulls on the left side to avoid mixing them on the same page.

3. **User-Defined Splitting**: For non-null values, invokes gistUserPicksplit to apply the opclass-specific splitting method with don't-care tuple optimization.

4. **Recursive Processing**: When the current column split is suboptimal, recursively processes subsequent columns to optimize don't-care tuple placement or handle degenerate splits.

5. **Union Key Computation**: Ensures correct union keys are computed for all columns, especially important for multi-column indexes and after recursive splitting.

The function handles edge cases like all-null columns, mixed null/non-null scenarios, and optimizes splits through intelligent tuple redistribution based on multiple column criteria.

## Parameters / Member Variables
- : The index relation being split
- : The page being split (used for entry initialization)
- : Array of IndexTuples to be processed (must contain at least 2 tuples)
- : Number of IndexTuples in the array
- : GiST state containing operator class methods and tuple descriptors
- : Working state and output area containing split vectors and union keys
- : Current column being processed (zero-based, initially 0 from external caller)

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (memory allocation)
  - [index_getattr](../i/index_getattr.md) (extract column values from tuples)
  - gistdentryinit (initialize GIST entries)
  - [gistSplitHalf](gistSplitHalf.md) (fallback even split)
  - [gistUserPicksplit](gistUserPicksplit.md) (user-defined splitting with optimization)
  - [gistunionsubkey](gistunionsubkey.md) (union key computation)
  - memcpy (memory copying for backup operations)
- Types referenced:
  - [Relation](../R/Relation.md), Page, IndexTuple
  - [GISTSTATE](../G/GISTSTATE.md), GistSplitVector, GIST_SPLITVEC
  - [GistEntryVector](../G/GistEntryVector.md), GISTENTRY
  - OffsetNumber, Datum
- Constants used:
  - GEVHDRSZ (GistEntryVector header size)
- Called from:
  - [gistSplit](gistSplit.md) (main external caller)
  - [gistSplitByKey](gistSplitByKey.md) (recursive self-calls)

## Notes and Other Information
- The function is designed to be called initially with attno=0, with internal recursion incrementing attno
- Handles the complex case of don't-care tuples that can be optimally placed using subsequent columns
- Implements sophisticated backup and restoration of split vectors during recursive processing
- Ensures union keys are correctly computed at the top level (attno=0) for multi-column indexes  
- The caller must initialize spl_lisnull and spl_risnull arrays to all-true before calling
- Uses a mapping system to track tuple positions during recursive splitting operations
- Designed to handle any number of index columns through recursive processing