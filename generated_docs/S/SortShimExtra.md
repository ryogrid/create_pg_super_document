# SortShimExtra

## Location
src/backend/utils/sort/sortsupport.c: 31 - 32

## Overview
SortShimExtra is a structure used to store information needed to use an old-style comparison function as a sort comparator, providing a bridge between traditional PostgreSQL btree comparison functions and the modern SortSupport framework.

## Definition


## Detailed Description
SortShimExtra serves as an adapter structure that enables old-style btree comparison functions to work with PostgreSQL's newer SortSupport infrastructure. This structure contains the necessary metadata and call context to efficiently invoke traditional comparison functions within the modern sorting framework.

The structure is designed for performance optimization by pre-initializing the function call information once and reusing it across multiple comparisons during sorting operations. This avoids the overhead of repeatedly setting up function call contexts for each comparison.

The structure is allocated in the sort context's memory and is paired with a shim function (comparison_shim) that handles the actual function invocation using the stored metadata.

## Parameters / Member Variables
- : FmgrInfo structure containing lookup data and metadata for the comparison function, including the function OID and other call-related information
- : FunctionCallInfoBaseData structure that serves as a reusable function call context, pre-initialized with collation information and argument slots for the two values being compared

## Dependencies
- Functions called/Symbols referenced:
  - FmgrInfo (PostgreSQL function manager info structure)
  - FunctionCallInfoBaseData (base structure for function call information)
- Called from (representative examples):
  - comparison_shim (uses SortShimExtra to perform actual comparisons)
  - PrepareSortSupportComparisonShim (allocates and initializes SortShimExtra)
  - SizeForSortShimExtra (macro that calculates the memory size needed for allocation)

## Notes and Other Information
- The structure size is calculated dynamically using the SizeForSortShimExtra macro, which accounts for the variable-length fcinfo structure based on the number of arguments
- Used specifically for 2-argument comparison functions (as evidenced by the initialization in PrepareSortSupportComparisonShim)
- The structure enables backward compatibility, allowing existing btree comparison functions to work with the newer, more efficient SortSupport framework without modification
- Memory for this structure is allocated in the sort context (ssup->ssup_cxt) to ensure proper cleanup when sorting operations complete
- The fcinfo structure is pre-initialized once with collation information and argument nullness indicators to minimize per-comparison overhead