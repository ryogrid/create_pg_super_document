# dependency_degree

## Location
[src/backend/statistics/dependencies.c:221-347](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/dependencies.c#L221-L347)

## Overview
Validates functional dependency on the data by determining the degree to which a set of columns functionally determines another column, returning a confidence score between 0 and 1.

## Definition

```c
static double
dependency_degree(StatsBuildData *data, int k, AttrNumber *dependency)
```
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

## Simplified Source

```c
static double dependency_degree(StatsBuildData *data, int k, AttrNumber *dependency) {
    int nitems;
    MultiSortSupport mss;
    SortItem *items;
    AttrNumber *attnums_dep;
    int group_size = 0;
    int n_violations = 0;
    int n_supporting_rows = 0;

    // Validate inputs and setup multi-sort support
    Assert(k >= 2);
    mss = multi_sort_init(k);

    // Translate dependency indexes to attribute numbers
    attnums_dep = (AttrNumber *) palloc(k * sizeof(AttrNumber));
    for (int i = 0; i < k; i++)
        attnums_dep[i] = data->attnums[dependency[i]];

    // Setup sort dimensions for all k columns
    for (int i = 0; i < k; i++) {
        VacAttrStats *colstat = data->stats[dependency[i]];
        TypeCacheEntry *type = lookup_type_cache(colstat->attrtypid, TYPECACHE_LT_OPR);

        if (type->lt_opr == InvalidOid)
            elog(ERROR, "cache lookup failed for ordering operator for type %u",
                 colstat->attrtypid);

        multi_sort_add_dimension(mss, i, type->lt_opr, colstat->attrcollid);
    }

    // Build sorted array of data items
    items = build_sorted_items(data, &nitems, mss, k, attnums_dep);

    // Walk through sorted data, grouping by first (k-1) columns
    group_size = 1;

    for (int i = 1; i <= nitems; i++) {
        // Check if group ended (reached end or different first k-1 values)
        if (i == nitems ||
            multi_sort_compare_dims(0, k - 2, &items[i - 1], &items[i], mss) != 0) {

            // If no violations in group, count as supporting dependency
            if (n_violations == 0)
                n_supporting_rows += group_size;

            // Reset for next group
            n_violations = 0;
            group_size = 1;
            continue;
        }
        // Check if last column differs (violation of dependency)
        else if (multi_sort_compare_dim(k - 1, &items[i - 1], &items[i], mss) != 0) {
            n_violations++;
        }

        group_size++;
    }

    // Return confidence score (supporting rows / total rows)
    return (n_supporting_rows * 1.0 / data->numrows);
}
```