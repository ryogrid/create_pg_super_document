# build_mss

## Location
[src/backend/statistics/mcv.c:347-378](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/mcv.c#L347-L378)

## Overview
Constructs a MultiSortSupport structure for multi-column sorting operations in statistical analysis, setting up the necessary sorting infrastructure for all attributes in the statistics build data.

## Definition
```c
static MultiSortSupport build_mss(StatsBuildData *data)
```

## Detailed Description
This function creates a MultiSortSupport object that enables simultaneous sorting across multiple columns. It initializes the sorting infrastructure by:

1. Creating a MultiSortSupport structure for the required number of attributes
2. For each attribute, looking up the less-than operator from the type cache
3. Adding each dimension to the MultiSortSupport with the appropriate operator and collation

The function ensures that all necessary sorting operators are available and properly configured for the data types involved. It performs error checking to verify that the less-than operator exists for each data type, which is essential for the sorting operations used in MCV list construction.

## Parameters / Member Variables
- `data`: StatsBuildData containing the attributes and statistics information for which sorting support is needed

## Dependencies
- Functions called/Symbols referenced:
  - [multi_sort_init](../m/multi_sort_init.md)
  - [lookup_type_cache](../l/lookup_type_cache.md)
  - [multi_sort_add_dimension](../m/multi_sort_add_dimension.md)
  - [VacAttrStats](../V/VacAttrStats.md), StatsBuildData, MultiSortSupport
  - TYPECACHE_LT_OPR
- Called from (representative examples):
  - [statext_mcv_build](../s/statext_mcv_build.md)
  - SizeOfMCVList

## Notes and Other Information
- Throws an ERROR if the less-than operator lookup fails for any data type
- Uses the default collation specified in the column statistics (attrcollid)
- The resulting MultiSortSupport can be used for complex multi-column sorting operations
- Essential infrastructure component for MCV list construction and other multi-column statistical operations
- Handles the complexity of setting up sort comparisons for heterogeneous column types

## Simplified Source

```c
static MultiSortSupport
build_mss(StatsBuildData *data)
{
    int i;
    int numattrs = data->nattnums;

    // Initialize multi-column sort support
    MultiSortSupport mss = multi_sort_init(numattrs);

    // Set up sort functions for each attribute
    for (i = 0; i < numattrs; i++)
    {
        VacAttrStats *colstat = data->stats[i];
        TypeCacheEntry *type;

        // Look up the less-than operator for this data type
        type = lookup_type_cache(colstat->attrtypid, TYPECACHE_LT_OPR);
        if (type->lt_opr == InvalidOid)
            elog(ERROR, "cache lookup failed for ordering operator for type %u",
                 colstat->attrtypid);

        // Add this dimension to the multi-sort support
        multi_sort_add_dimension(mss, i, type->lt_opr, colstat->attrcollid);
    }

    return mss;
}
```