# PrepareSortSupportComparisonShim

## Location
src/backend/utils/sort/sortsupport.c: 68 - 93

## Overview
Sets up a shim function to allow old-style btree comparison functions to work seamlessly with PostgreSQL's modern SortSupport framework.

## Definition


## Detailed Description
PrepareSortSupportComparisonShim serves as a bridge between legacy btree comparison functions and the modern SortSupport infrastructure. This function:

1. Allocates memory for a SortShimExtra structure in the appropriate memory context
2. Looks up and caches information about the comparison function using fmgr_info_cxt
3. Pre-initializes a FunctionCallInfoData structure that can be reused for multiple comparisons
4. Sets up the comparison_shim as the comparator function in the SortSupport structure

This approach provides significant performance benefits by avoiding the overhead of repeatedly setting up function call information for each comparison operation. Instead, the setup is done once, and the lightweight comparison_shim function handles individual comparisons efficiently.

## Parameters / Member Variables
- : OID of the btree comparison function to be wrapped
- : SortSupport structure to be configured with the shim

## Dependencies
- Functions called/Symbols referenced:
  - SortSupport (type)
  - SortShimExtra (type)
  - MemoryContextAlloc
  - SizeForSortShimExtra
  - fmgr_info_cxt
  - InitFunctionCallInfoData
  - comparison_shim
- Called from:
  - MJExamineQuals (at src/backend/executor/nodeMergejoin.c:262)
  - FinishSortSupportFunction (at src/backend/utils/sort/sortsupport.c:122)
  - ApplySortAbbrevFullComparator (at src/include/utils/sortsupport.h:385)

## Notes and Other Information
- This function enables backward compatibility with older comparison functions that don't natively support SortSupport
- Memory allocation uses the SortSupport's own memory context to ensure proper cleanup
- The pre-initialization of function call structures is a key optimization that reduces per-comparison overhead
- Both function arguments are marked as non-null in the pre-initialized structure
- Part of PostgreSQL's performance optimization strategy for sorting operations