# FinishSortSupportFunction

## Location
src/backend/utils/sort/sortsupport.c: 94 - 133

## Overview
A static function that attempts to set up an optimal SortSupport comparator by first trying to find a native sort support function, and falling back to a compatibility shim if needed.

## Definition


## Detailed Description
FinishSortSupportFunction implements a two-tier strategy for setting up sort comparison functionality:

1. **Primary Strategy**: First attempts to find and call a dedicated sort support function (BTSORTSUPPORT_PROC) from the operator family. These functions can provide highly optimized, type-specific comparison logic and may also set up additional optimizations like abbreviation keys.

2. **Fallback Strategy**: If no sort support function exists or if the sort support function declines to set up a comparator (returning without setting ssup->comparator), the function falls back to using a traditional btree comparison function (BTORDER_PROC) wrapped in a compatibility shim.

This design allows PostgreSQL to gradually transition from old-style comparison functions to the more efficient SortSupport framework while maintaining full backward compatibility. The sort support functions can make runtime decisions about whether to provide optimized comparators based on factors like collation settings.

## Parameters / Member Variables
- : OID of the operator family to search for comparison functions
- : OID of the input data type for the comparison operations
- : SortSupport structure to be configured with the appropriate comparator

## Dependencies
- Functions called/Symbols referenced:
  - SortSupport (type)
  - get_opfamily_proc
  - BTSORTSUPPORT_PROC
  - OidFunctionCall1
  - BTORDER_PROC
  - PrepareSortSupportComparisonShim
- Called from:
  - PrepareSortSupportFromOrderingOp (at src/backend/utils/sort/sortsupport.c:149)
  - PrepareSortSupportFromIndexRel (at src/backend/utils/sort/sortsupport.c:176)

## Notes and Other Information
- This is a static function, only accessible within sortsupport.c
- The function provides a graceful degradation path from modern sort support functions to legacy comparison functions
- Sort support functions have the flexibility to decline providing a comparator, allowing for conditional optimization
- Missing BTORDER_PROC functions result in an error, as basic comparison functionality is mandatory
- Part of PostgreSQL's extensible operator system that allows data types to provide custom sorting optimizations