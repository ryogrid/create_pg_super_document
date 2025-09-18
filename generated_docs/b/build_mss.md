# build_mss

## Location
src/backend/statistics/mcv.c: 347 - 378

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
  - multi_sort_init
  - lookup_type_cache
  - multi_sort_add_dimension
  - VacAttrStats, StatsBuildData, MultiSortSupport
  - TYPECACHE_LT_OPR
- Called from (representative examples):
  - statext_mcv_build
  - SizeOfMCVList

## Notes and Other Information
- Throws an ERROR if the less-than operator lookup fails for any data type
- Uses the default collation specified in the column statistics (attrcollid)
- The resulting MultiSortSupport can be used for complex multi-column sorting operations
- Essential infrastructure component for MCV list construction and other multi-column statistical operations
- Handles the complexity of setting up sort comparisons for heterogeneous column types