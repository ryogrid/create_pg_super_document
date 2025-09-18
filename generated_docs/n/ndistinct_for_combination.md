# ndistinct_for_combination

## Location
[src/backend/statistics/mvdistinct.c:425-520](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/mvdistinct.c#L425-L520)

## Overview
A static function that estimates the number of distinct values in a combination of columns using PostgreSQL's standard n-distinct estimator algorithm.

## Definition


## Detailed Description
This function estimates the number of distinct values for a specific combination of columns using the same n-distinct estimator as PostgreSQL's ANALYZE command. It implements the formula: n*d / (n - f1 + f1*n/N), where n is the sample size, d is the number of distinct values in the sample, f1 is the number of values that appear exactly once, and N is the total number of rows.

The function works by:
1. Creating a multi-dimensional sort infrastructure for the specified column combination
2. Copying and organizing sample data for each dimension in the combination  
3. Setting up sort support for each column using their default sort operators
4. Sorting the combined data to identify distinct combinations
5. Counting distinct values and singletons (f1) for the n-distinct estimation
6. Applying the standard n-distinct formula to estimate total distinct combinations

## Parameters / Member Variables
- : Total number of rows in the table
- : StatsBuildData structure containing sample data for statistics building
- : Number of columns in the combination
- : Array of column attribute numbers that form the combination

## Dependencies
- Functions called/Symbols referenced:
  - [multi_sort_init](../m/multi_sort_init.md) (initializes multi-dimensional sorting)
  - [palloc](../p/palloc.md), palloc0 (PostgreSQL memory allocation functions)
  - [lookup_type_cache](../l/lookup_type_cache.md) (retrieves type cache information)
  - [multi_sort_add_dimension](../m/multi_sort_add_dimension.md) (adds dimension to multi-sort support)
  - qsort_interruptible (interruptible quicksort implementation)
  - [multi_sort_compare](../m/multi_sort_compare.md) (comparison function for multi-dimensional data)
  - [estimate_ndistinct](../e/estimate_ndistinct.md) (applies n-distinct estimation formula)
- Data types used:
  - [StatsBuildData](../S/StatsBuildData.md) (statistics building data structure)
  - [SortItem](../S/SortItem.md) (structure for sortable items with multiple dimensions)
  - MultiSortSupport (multi-dimensional sort support)
  - VacAttrStats (column statistics structure)
  - [TypeCacheEntry](../T/TypeCacheEntry.md) (type cache information)
- Called from:
  - statext_ndistinct_build (builds n-distinct statistics for column combinations)

## Notes and Other Information
- This is a static function only used within mvdistinct.c
- Uses the same estimator algorithm as PostgreSQL's single-column ANALYZE statistics
- Handles multi-dimensional sorting by setting up sort support for each column in the combination
- The function uses default sort operators and collations for column types
- Memory is allocated for temporary arrays to avoid sorting sample data in place
- Located in src/backend/statistics/mvdistinct.c:425-520