# IndexVacuumInfo

## Location
[src/include/access/genam.h:44-54](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/genam.h#L44-L54)

## Overview
IndexVacuumInfo is a structure that contains input arguments passed to ambulkdelete and amvacuumcleanup functions during index vacuum and cleanup operations.

## Definition

```c
typedef struct IndexVacuumInfo
{
	Relation	index;			/* the index being vacuumed */
	Relation	heaprel;		/* the heap relation the index belongs to */
	bool		analyze_only;	/* ANALYZE (without any actual vacuum) */
	bool		report_progress;	/* emit progress.h status reports */
	bool		estimated_count;	/* num_heap_tuples is an estimate */
	int			message_level;	/* ereport level for progress messages */
	double		num_heap_tuples;	/* tuples remaining in heap */
	BufferAccessStrategy strategy;	/* access strategy for reads */
} IndexVacuumInfo;
```
## Detailed Description
IndexVacuumInfo serves as a parameter structure for index access method vacuum operations. It provides all necessary context and configuration for both bulk delete operations (ambulkdelete) and vacuum cleanup operations (amvacuumcleanup). The structure encapsulates information about the target index, its corresponding heap relation, operational modes, progress reporting settings, and access strategies.

A key aspect of this structure is the handling of tuple count estimates. The num_heap_tuples field is accurate only when estimated_count is false. When estimated_count is true, the value represents an estimate (typically from pg_class.reltuples) and may even be -1. During ambulkdelete operations, this will always be an estimate.

## Parameters / Member Variables
- `index`: The index relation being vacuumed
- `heaprel`: The heap relation that the index belongs to
- `analyze_only`: Boolean flag indicating this is an ANALYZE operation without actual vacuuming
- `report_progress`: Boolean flag to enable progress reporting via the progress.h mechanism
- `estimated_count`: Boolean flag indicating whether num_heap_tuples is an estimate or accurate count
- `message_level`: The ereport level to use for progress messages (e.g., DEBUG1, LOG, etc.)
- `num_heap_tuples`: The number of tuples remaining in the heap (may be estimated)
- `strategy`: Buffer access strategy to use for reading pages during vacuum operations
## Dependencies
- Functions called/Symbols referenced:
  - [BufferAccessStrategy](../B/BufferAccessStrategy.md) (for memory management strategy)
- Called from (representative examples):
  - [brinbulkdelete](../b/brinbulkdelete.md)/brinvacuumcleanup (BRIN index vacuum)
  - [ginbulkdelete](../g/ginbulkdelete.md)/ginvacuumcleanup (GIN index vacuum)
  - [gistbulkdelete](../g/gistbulkdelete.md)/gistvacuumcleanup (GiST index vacuum)
  - [hashbulkdelete](../h/hashbulkdelete.md)/hashvacuumcleanup (Hash index vacuum)
  - [btbulkdelete](../b/btbulkdelete.md)/btvacuumcleanup (B-tree index vacuum)
  - [spgbulkdelete](../s/spgbulkdelete.md)/spgvacuumcleanup (SP-GiST index vacuum)
  - [index_bulk_delete](../i/index_bulk_delete.md)/index_vacuum_cleanup (generic index vacuum functions)

## Notes and Other Information
- This structure is defined in src/include/access/genam.h and is used across all index access methods
- The estimated_count flag is crucial for understanding the reliability of the num_heap_tuples value
- Progress reporting can be controlled through the report_progress flag and message_level setting
- The BufferAccessStrategy helps optimize I/O patterns during vacuum operations
- Used in both parallel and non-parallel vacuum operations
- The analyze_only flag allows the same structure to be used for ANALYZE operations that don't perform actual vacuuming