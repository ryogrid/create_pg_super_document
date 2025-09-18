# dependency_degree

## Location
[src/backend/statistics/dependencies.c:221-347](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/dependencies.c#L221-L347)

## Overview
Validates functional dependency on the data by determining the degree to which a set of columns functionally determines another column, returning a confidence score between 0 and 1.

## Definition


## Detailed Description
This function is the core algorithm for detecting functional dependencies in PostgreSQL's multivariate statistics. Given a set of k attributes, it verifies whether the first (k-1) attributes are sufficient to functionally determine the last attribute. The function uses a sorting-based approach:

1. Sorts all rows lexicographically by all k columns
2. Groups rows by the first (k-1) columns 
3. For each group, checks if all rows have the same value in the last column
4. Returns the ratio of supporting rows to total rows as a confidence measure

The algorithm assumes that if A functionally determines B, then for any group of rows with identical A values, all B values should also be identical. Violations indicate the dependency is not perfect.

## Parameters / Member Variables
- : StatsBuildData structure containing the sample data and column information for statistics computation
- : Number of attributes in the dependency relationship (must be >= 2)
- : Array of attribute indexes representing the dependency (first k-1 determine the last one)

## Dependencies
- Functions called/Symbols referenced:
  - [multi_sort_init](../m/multi_sort_init.md)
  - [lookup_type_cache](../l/lookup_type_cache.md)  
  - [multi_sort_add_dimension](../m/multi_sort_add_dimension.md)
  - [build_sorted_items](../b/build_sorted_items.md)
  - [multi_sort_compare_dims](../m/multi_sort_compare_dims.md)
  - [multi_sort_compare_dim](../m/multi_sort_compare_dim.md)
- Called from:
  - [statext_dependencies_build](../s/statext_dependencies_build.md)
  - DependencyGenerator

## Notes and Other Information
- Uses PostgreSQL's multi-sort support for efficient lexicographic sorting
- Relies on column data types' default sort operators and collations
- The confidence score (0.0 to 1.0) represents the fraction of rows that support the functional dependency
- A score of 1.0 indicates a perfect functional dependency
- Currently assumes all statistics entries point to the same tuple descriptor
- Part of PostgreSQL's extended statistics framework for multivariate analysis