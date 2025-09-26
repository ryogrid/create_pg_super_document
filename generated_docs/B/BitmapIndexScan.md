# BitmapIndexScan

## Location
[src/include/nodes/plannodes.h:520-527](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/plannodes.h#L520-L527)

## Overview
BitmapIndexScan is a PostgreSQL plan node that scans an index to generate a bitmap of potential tuple locations without accessing the heap directly.

## Definition

```c
typedef struct BitmapIndexScan
{
	Scan		scan;
	Oid			indexid;		/* OID of index to scan */
	bool		isshared;		/* Create shared bitmap if set */
	List	   *indexqual;		/* list of index quals (OpExprs) */
	List	   *indexqualorig;	/* the same in original form */
} BitmapIndexScan;
```
## Detailed Description
BitmapIndexScan is a specialized scan node that delivers a bitmap of potential tuple locations rather than actual tuples. It operates as part of PostgreSQL's bitmap scan mechanism, where the bitmap it produces is consumed by an ancestor BitmapHeapScan node. The bitmap may pass through intermediate BitmapAnd and/or BitmapOr nodes to combine results from multiple BitmapIndexScans before reaching the heap scan.

Unlike regular IndexScan nodes, BitmapIndexScan does not store a direction flag since scan direction is irrelevant for bitmap generation. The targetlist and qual fields are not used in BitmapIndexScan plans and are always NIL, while indexqualorig is preserved only for EXPLAIN output.

## Parameters / Member Variables
- `scan`: Base Scan structure containing common scan node fields
- `indexid`: OID of the index to scan for generating the bitmap
- `isshared`: Boolean flag indicating whether to create a shared bitmap (used in parallel query execution)
- `*indexqual`: List of index qualification expressions (OpExprs) used to filter index entries
- `*indexqualorig`: Original form of index qualifications, preserved for EXPLAIN purposes
## Dependencies
- Functions called/Symbols referenced:
  - [Scan](../S/Scan.md) (base structure)
- Called from (representative examples):
  - [ExplainNode](../E/ExplainNode.md) (for EXPLAIN output)
  - [ExecInitNode](../E/ExecInitNode.md) (executor initialization)
  - [MultiExecBitmapIndexScan](../M/MultiExecBitmapIndexScan.md) (execution function)
  - [ExecInitBitmapIndexScan](../E/ExecInitBitmapIndexScan.md) (initialization)
  - [make_bitmap_indexscan](../m/make_bitmap_indexscan.md) (plan creation)
  - [bitmap_subplan_mark_shared](../b/bitmap_subplan_mark_shared.md) (parallel query planning)

## Notes and Other Information
- [BitmapIndexScan](BitmapIndexScan.md) nodes work in conjunction with BitmapHeapScan to implement PostgreSQL's bitmap scan strategy
- The bitmap produced can be combined with other bitmaps using BitmapAnd/BitmapOr operations
- This scan type is particularly efficient when multiple indexes need to be consulted or when the selectivity of the index condition results in scattered heap page accesses
- The isshared flag enables parallel execution by allowing multiple worker processes to contribute to a shared bitmap structure