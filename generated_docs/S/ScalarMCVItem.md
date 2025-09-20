# ScalarMCVItem

## Location
[src/backend/commands/analyze.c:1810-1815](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/analyze.c#L1810-L1815)

## Overview
A structure used during PostgreSQL's ANALYZE command to track Most Common Value (MCV) items for scalar data types during statistical analysis of table columns.

## Definition

```c
typedef struct
{
	SortSupport ssup;
	int		   *tupnoLink;
} CompareScalarsContext;
```
## Detailed Description
ScalarMCVItem is an internal data structure used by PostgreSQL's ANALYZE command when computing statistics for scalar (non-array) column data types. It serves as a tracking mechanism for identifying and managing the most frequently occurring values in a column sample. Each ScalarMCVItem instance represents a distinct value that has been identified as a candidate for inclusion in the Most Common Values (MCV) list, which is stored in the pg_statistic system catalog and used by the query planner for cardinality estimation.

The structure is used during the statistical analysis phase where PostgreSQL sorts sample values, identifies duplicates, and determines which values appear most frequently. This information is crucial for the query optimizer to make informed decisions about join strategies, index usage, and row count estimates.

## Parameters / Member Variables
- `ssup`: The number of times this particular value appears in the analyzed sample (i.e., the frequency count of duplicates)
- `*tupnoLink`: An index into the values[] array pointing to the first occurrence of this value in the sorted sample data
## Dependencies
- Functions called/Symbols referenced:
  - Used within compute_scalar_stats function for MCV analysis
- Called from (representative examples):
  - [compute_scalar_stats](../c/compute_scalar_stats.md) (allocated and used for MCV tracking)
  - [compare_mcvs](../c/compare_mcvs.md) (used in qsort comparison function)

## Notes and Other Information
- This structure is part of PostgreSQL's internal statistics collection system and is not exposed to user-level SQL
- The structure is allocated in arrays during ANALYZE operations and freed after statistics computation
- MCV items are sorted by position order before being processed to optimize subsequent operations
- The tracked values are later collapsed from the main values array to create the final MCV list stored in pg_statistic
- This is specifically used for scalar types; array types have separate handling mechanisms