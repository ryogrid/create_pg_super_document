# StatsBuildData

## Location
[src/include/statistics/extended_stats_internal.h:61-69](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/statistics/extended_stats_internal.h#L61-L69)

## Overview
StatsBuildData is a unified structure that encapsulates all the essential data needed for building PostgreSQL extended statistics, providing a standardized interface for various statistical computation algorithms.

## Definition

```c
typedef struct StatsBuildData
{
	int			numrows;
	int			nattnums;
	AttrNumber *attnums;
	VacAttrStats **stats;
	Datum	  **values;
	bool	  **nulls;
} StatsBuildData;
```
## Detailed Description
StatsBuildData serves as a central data container that provides a unified representation of the raw data used for building various types of extended statistics in PostgreSQL. This structure aggregates all necessary information including the actual data values, NULL indicators, attribute information, and statistical metadata into a single, convenient interface. It enables different statistical algorithms (MCV, dependencies, n-distinct) to work with a consistent data format, promoting code reuse and maintainability. The structure organizes data in a column-wise format where each attribute's values and null indicators are stored in separate arrays, facilitating efficient processing of multi-column statistics.

## Parameters / Member Variables
- `numrows`: The total number of rows (tuples) in the dataset being analyzed
- `nattnums`: The number of attributes (columns) included in the statistics
- `*attnums`: Pointer to an array of AttrNumber values identifying the specific columns being analyzed
- `**stats`: Pointer to an array of VacAttrStats pointers, containing per-attribute statistical information and metadata
- `**values`: Pointer to a two-dimensional array where values[i][j] contains the j-th row's value for the i-th attribute
- `**nulls`: Pointer to a two-dimensional boolean array where nulls[i][j] indicates if the j-th row's value for the i-th attribute is NULL
## Dependencies
- Functions called/Symbols referenced:
  - [VacAttrStats](../V/VacAttrStats.md) (referenced for per-attribute statistics metadata)
- Called from (representative examples):
  - DependencyGenerator (src/backend/statistics/dependencies.c:73)
  - [dependency_degree](../d/dependency_degree.md) (src/backend/statistics/dependencies.c:221)
  - [statext_dependencies_build](../s/statext_dependencies_build.md) (src/backend/statistics/dependencies.c:348)
  - [BuildRelationExtStatistics](../B/BuildRelationExtStatistics.md) (src/backend/statistics/extended_stats.c:163)
  - [build_sorted_items](../b/build_sorted_items.md) (src/backend/statistics/extended_stats.c:986)
  - [make_build_data](../m/make_build_data.md) (src/backend/statistics/extended_stats.c:2456, 2471, 2486, 2487)
  - [statext_mcv_build](../s/statext_mcv_build.md) (src/backend/statistics/mcv.c:180)
  - [build_mss](../b/build_mss.md) (src/backend/statistics/mcv.c:347)
  - [statext_ndistinct_build](../s/statext_ndistinct_build.md) (src/backend/statistics/mvdistinct.c:88)
  - [ndistinct_for_combination](../n/ndistinct_for_combination.md) (src/backend/statistics/mvdistinct.c:425)

## Notes and Other Information
- Central data structure for all extended statistics building operations in PostgreSQL
- Provides a unified interface that abstracts the complexity of multi-column data organization
- Enables efficient column-wise access patterns needed for statistical computations
- Used across multiple statistics types: dependencies, MCV lists, and n-distinct estimates
- The two-dimensional array layout (values[attr][row]) optimizes for attribute-wise processing
- Integrates with PostgreSQL's ANALYZE infrastructure through VacAttrStats
- Located in src/include/statistics/extended_stats_internal.h as part of the core extended statistics framework
- Essential for the make_build_data function that constructs these structures from raw table data