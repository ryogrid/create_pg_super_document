# statext_is_kind_built

## Location
[src/backend/statistics/extended_stats.c:389-421](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/extended_stats.c#L389-L421)

## Overview
statext_is_kind_built checks whether a specific type of extended statistics has been computed and stored in a given pg_statistic_ext_data tuple.

## Definition

```c
bool
statext_is_kind_built(HeapTuple htup, char type)
```
## Detailed Description
This function determines if a particular kind of extended statistics exists in a pg_statistic_ext_data catalog tuple by checking if the corresponding attribute is non-NULL. It supports four types of extended statistics: n-distinct (NDISTINCT), functional dependencies (DEPENDENCIES), most common values (MCV), and expression statistics (EXPRESSIONS). The function maps each statistics type to its corresponding catalog column and uses heap_attisnull to check for the presence of data.

## Parameters / Member Variables
- : HeapTuple from pg_statistic_ext_data catalog containing statistics data
- : Character code indicating which statistics type to check (STATS_EXT_NDISTINCT, STATS_EXT_DEPENDENCIES, STATS_EXT_MCV, or STATS_EXT_EXPRESSIONS)

## Dependencies
- Functions called/Symbols referenced:
  - [heap_attisnull](../h/heap_attisnull.md)
  - STATS_EXT_NDISTINCT (constant)
  - STATS_EXT_DEPENDENCIES (constant) 
  - STATS_EXT_MCV (constant)
  - STATS_EXT_EXPRESSIONS (constant)
  - Anum_pg_statistic_ext_data_stxdndistinct (catalog column number)
  - Anum_pg_statistic_ext_data_stxddependencies (catalog column number)
  - Anum_pg_statistic_ext_data_stxdmcv (catalog column number)
  - Anum_pg_statistic_ext_data_stxdexpr (catalog column number)
- Called from:
  - [get_relation_statistics_worker](../g/get_relation_statistics_worker.md) (in src/backend/optimizer/util/plancat.c at lines 1402, 1416, 1430, 1444)

## Notes and Other Information
- Returns true if the requested statistics type has been computed and stored
- Returns false if the statistics data is NULL (not computed or disabled)
- Raises an ERROR for invalid/unexpected statistics type codes
- Used by the query planner to determine which extended statistics are available for use
- Maps statistics types to their corresponding pg_statistic_ext_data catalog columns
- Part of the extended statistics infrastructure for multi-column statistics

## Simplified Source

```c
bool
statext_is_kind_built(HeapTuple htup, char type)
{
    AttrNumber attnum;

    // Map statistics type to corresponding catalog column
    switch (type) {
        case STATS_EXT_NDISTINCT:
            attnum = Anum_pg_statistic_ext_data_stxdndistinct;
            break;

        case STATS_EXT_DEPENDENCIES:
            attnum = Anum_pg_statistic_ext_data_stxddependencies;
            break;

        case STATS_EXT_MCV:
            attnum = Anum_pg_statistic_ext_data_stxdmcv;
            break;

        case STATS_EXT_EXPRESSIONS:
            attnum = Anum_pg_statistic_ext_data_stxdexpr;
            break;

        default:
            elog(ERROR, "unexpected statistics type requested: %d", type);
    }

    // Check if the attribute is non-NULL (statistics exist)
    return !heap_attisnull(htup, attnum, NULL);
}
```