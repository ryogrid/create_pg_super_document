# BitmapIndexScan

## Location
src/include/nodes/plannodes.h: 520 - 527

## Overview
BitmapIndexScan is a PostgreSQL plan node that scans an index to generate a bitmap of potential tuple locations without accessing the heap directly.

## Definition


## Detailed Description
BitmapIndexScan is a specialized scan node that delivers a bitmap of potential tuple locations rather than actual tuples. It operates as part of PostgreSQL's bitmap scan mechanism, where the bitmap it produces is consumed by an ancestor BitmapHeapScan node. The bitmap may pass through intermediate BitmapAnd and/or BitmapOr nodes to combine results from multiple BitmapIndexScans before reaching the heap scan.

Unlike regular IndexScan nodes, BitmapIndexScan does not store a direction flag since scan direction is irrelevant for bitmap generation. The targetlist and qual fields are not used in BitmapIndexScan plans and are always NIL, while indexqualorig is preserved only for EXPLAIN output.

## Parameters / Member Variables
- : Base Scan structure containing common scan node fields
- : OID of the index to scan for generating the bitmap
- : Boolean flag indicating whether to create a shared bitmap (used in parallel query execution)
- : List of index qualification expressions (OpExprs) used to filter index entries
- : Original form of index qualifications, preserved for EXPLAIN purposes

## Dependencies
- Functions called/Symbols referenced:
  - Scan (base structure)
- Called from (representative examples):
  - ExplainNode (for EXPLAIN output)
  - ExecInitNode (executor initialization)
  - MultiExecBitmapIndexScan (execution function)
  - ExecInitBitmapIndexScan (initialization)
  - make_bitmap_indexscan (plan creation)
  - bitmap_subplan_mark_shared (parallel query planning)

## Notes and Other Information
- BitmapIndexScan nodes work in conjunction with BitmapHeapScan to implement PostgreSQL's bitmap scan strategy
- The bitmap produced can be combined with other bitmaps using BitmapAnd/BitmapOr operations
- This scan type is particularly efficient when multiple indexes need to be consulted or when the selectivity of the index condition results in scattered heap page accesses
- The isshared flag enables parallel execution by allowing multiple worker processes to contribute to a shared bitmap structure