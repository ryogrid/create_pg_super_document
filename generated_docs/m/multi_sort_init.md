# multi_sort_init

## Location
src/backend/statistics/extended_stats.c: 832 - 850

## Overview
Initializes a multi-dimensional sort support structure used for sorting tuples across multiple columns in PostgreSQL's extended statistics computations.

## Definition


## Detailed Description
The multi_sort_init function creates and initializes a MultiSortSupport structure that enables efficient multi-dimensional sorting operations required by PostgreSQL's extended statistics system. This function is essential for statistical computations that need to sort data across multiple columns simultaneously, such as computing functional dependencies, most common values (MCV), and n-distinct statistics.

The function performs key initialization tasks:
1. **Memory Allocation**: Allocates memory for the MultiSortSupport structure including space for multiple SortSupportData elements
2. **Dimension Setup**: Records the number of dimensions (columns) that will be involved in the sort operation
3. **Structure Preparation**: Uses palloc0 to zero-initialize the structure, ensuring all fields start in a clean state

The resulting structure provides a framework for setting up sort operations across multiple columns, where each dimension can have its own comparison function and sort criteria. This is particularly important for extended statistics where relationships between multiple columns need to be analyzed through sorted data access patterns.

## Parameters / Member Variables
- : The number of dimensions (columns) for the multi-dimensional sort; must be at least 2

## Dependencies
- Functions called/Symbols referenced:
  - MultiSortSupport, MultiSortSupportData (type definitions for sort support)
  - SortSupportData (individual column sort support structure)
  - palloc0 (zero-initialized memory allocation)
  - offsetof (structure offset calculation macro)
- Called from (representative examples):
  - dependency_degree (functional dependency computation)
  - build_mss (MCV list building)
  - ndistinct_for_combination (n-distinct statistics calculation)

## Notes and Other Information
- Requires at least 2 dimensions (enforced by Assert)
- Uses flexible array member technique with offsetof to allocate variable-sized structure
- Zero-initialization ensures all SortSupportData elements start in clean state
- The returned structure needs further configuration with comparison functions for each dimension
- Memory allocation includes space for both the base structure and the array of SortSupportData elements
- Part of PostgreSQL's infrastructure for computing multi-variate statistics
- The function is lightweight and only handles initial allocation - actual sort configuration happens separately
- Used across different extended statistics modules (dependencies, MCV, n-distinct) indicating its fundamental role in multi-column analysis