# AnlIndexData

## Location
[src/backend/commands/analyze.c:63-69](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/analyze.c#L63-L69)

## Overview
AnlIndexData is a structure that holds per-index data used during the ANALYZE command execution in PostgreSQL, containing information needed for analyzing index statistics.

## Definition

```c
typedef struct AnlIndexData
{
	IndexInfo  *indexInfo;		/* BuildIndexInfo result */
	double		tupleFract;		/* fraction of rows for partial index */
	VacAttrStats **vacattrstats;	/* index attrs to analyze */
	int			attr_cnt;
} AnlIndexData;
```
## Detailed Description
AnlIndexData serves as a container for index-specific information during ANALYZE operations. This structure is used to collect and organize data necessary for computing statistics on indexes, including both complete and partial indexes. It maintains references to the index metadata, statistical analysis structures for individual attributes, and fraction information for partial indexes that only cover a subset of table rows.

The structure is primarily used within the analyze.c module to coordinate the collection of index statistics, ensuring that the ANALYZE command can properly evaluate index usage patterns and data distribution for query planning optimization.

## Parameters / Member Variables
- `*indexInfo`: Pointer to IndexInfo structure containing the result from BuildIndexInfo, which holds metadata about the index structure and properties
- `tupleFract`: Double precision value representing the fraction of table rows that are covered by this index (important for partial indexes)
- `**vacattrstats`: Array of pointers to VacAttrStats structures, one for each index attribute that needs statistical analysis
- `attr_cnt`: Integer count of the number of attributes in the index that require analysis
## Dependencies
- Functions called/Symbols referenced:
  - [IndexInfo](../I/IndexInfo.md)
  - [VacAttrStats](../V/VacAttrStats.md)
- Called from (representative examples):
  - [do_analyze_rel](../d/do_analyze_rel.md) (multiple references for index processing)
  - [compute_index_stats](../c/compute_index_stats.md) (for statistical computation)

## Notes and Other Information
- This structure is defined at src/backend/commands/analyze.c:63-69
- Used extensively in the do_analyze_rel function for managing multiple indexes during table analysis
- The tupleFract member is particularly important for partial indexes where statistics need to be weighted appropriately
- The structure facilitates parallel processing of multiple indexes by organizing per-index data separately
- Memory management for the vacattrstats array and its contents is handled by the calling functions