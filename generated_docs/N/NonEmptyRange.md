# NonEmptyRange

## Location
src/backend/utils/adt/rangetypes_gist.c: 98 - 109

## Overview
A structure that holds the extracted lower and upper bounds from a non-empty range, used specifically in the range_gist_double_sorting_split algorithm.

## Definition
```c
typedef struct
{
	RangeBound	lower;
	RangeBound	upper;
} NonEmptyRange;
```

## Detailed Description
NonEmptyRange is a simple but important data structure used in PostgreSQL's GiST implementation for range types. It represents the bounds extracted from a non-empty range during the double sorting split algorithm. This structure serves as a simplified representation of range data, containing only the essential boundary information needed for split calculations. By extracting and storing just the bounds, it enables efficient processing during the complex range splitting operations without carrying the full overhead of complete range objects.

## Parameters / Member Variables
- `lower`: The lower bound of the range, represented as a RangeBound structure
- `upper`: The upper bound of the range, represented as a RangeBound structure

## Dependencies
- Functions called/Symbols referenced:
  - RangeBound (range boundary structure)
- Called from (representative examples):
  - range_gist_double_sorting_split (primary usage)
  - interval_cmp_lower (comparison function)
  - interval_cmp_upper (comparison function)

## Notes and Other Information
- Defined in src/backend/utils/adt/rangetypes_gist.c:94-98
- Specifically designed for use with the range_gist_double_sorting_split algorithm
- Provides a lightweight representation focusing only on boundary information
- Part of PostgreSQL's range type indexing infrastructure
- Used during the sorting and splitting phases of range-based GiST index operations
- Essential for maintaining performance during complex range partitioning operations