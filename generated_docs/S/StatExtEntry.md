# StatExtEntry

## Location
[src/backend/statistics/extended_stats.c:64-73](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/extended_stats.c#L64-L73)

## Overview
StatExtEntry is an internal structure used to represent an individual extended statistics object, corresponding to an entry in the pg_statistic_ext system catalog.

## Definition
```c
typedef struct StatExtEntry
{
    Oid         statOid;        /* OID of pg_statistic_ext entry */
    char       *schema;         /* statistics object's schema */
    char       *name;           /* statistics object's name */
    Bitmapset  *columns;        /* attribute numbers covered by the object */
    List       *types;          /* 'char' list of enabled statistics kinds */
    int         stattarget;     /* statistics target (-1 for default) */
    List       *exprs;          /* expressions */
} StatExtEntry;
```

## Detailed Description
StatExtEntry serves as an internal representation of extended statistics objects within PostgreSQL's statistics collection system. This structure consolidates all the essential information needed to process and manage extended statistics objects during query analysis and optimization. It acts as a bridge between the system catalog information (pg_statistic_ext) and the runtime statistics processing algorithms.

The structure encapsulates both column-based and expression-based statistics, supporting various types of extended statistics such as dependencies, distinct value counts (n-distinct), and most common values (MCV) lists. It is primarily used during the extended statistics collection phase when analyzing relations.

## Parameters / Member Variables
- `statOid`: The object identifier (OID) that uniquely identifies this statistics object in the pg_statistic_ext system catalog
- `schema`: Name of the schema containing the statistics object, stored as a null-terminated string
- `name`: Name of the statistics object itself, stored as a null-terminated string  
- `columns`: A bitmapset containing the attribute numbers (column positions) that this statistics object covers
- `types`: A list of characters representing the types of extended statistics enabled for this object (e.g., 'd' for dependencies, 'f' for functional dependencies, 'm' for MCV lists)
- `stattarget`: The statistics target value controlling the amount of statistics collected (-1 indicates using the default target)
- `exprs`: A list of expressions for which statistics should be collected, used for expression-based extended statistics

## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
  - [Bitmapset](../B/Bitmapset.md) (PostgreSQL bitmap set data structure)
  - [List](../L/List.md) (PostgreSQL list data structure)

- Called from (representative examples):
  - [BuildRelationExtStatistics](../B/BuildRelationExtStatistics.md)
  - [ComputeExtStatisticsRows](../C/ComputeExtStatisticsRows.md)
  - [fetch_statentries_for_relation](../f/fetch_statentries_for_relation.md)
  - [make_build_data](../m/make_build_data.md)

## Notes and Other Information
This structure is defined in src/backend/statistics/extended_stats.c and is used exclusively within the extended statistics subsystem. It represents a runtime view of statistics objects and is not directly exposed to SQL users. The structure facilitates efficient processing of extended statistics during both collection and utilization phases of query planning and execution.

The stattarget field allows fine-grained control over statistics collection granularity, with higher values resulting in more detailed statistics at the cost of increased storage and collection time.