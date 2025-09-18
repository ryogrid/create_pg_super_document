# BulkInsertState

## Location
src/include/access/heapam.h: 44 - 47

## Overview
BulkInsertState is a typedef for BulkInsertStateData pointer that manages state information during bulk insertion operations, providing optimizations for buffer management and page extensions in heap relations.

## Definition


## Detailed Description
BulkInsertState maintains critical state information for optimizing bulk insertion operations in PostgreSQL heap relations. It implements a specialized buffer access strategy (BULKWRITE) to minimize buffer pool churn during large data loads. The structure tracks the current insertion target page and manages bulk page extensions to reduce the overhead of repeated relation extensions. When multiple pages are needed, it pre-extends the relation and keeps track of available free pages, allowing subsequent insertions to reuse these pages efficiently.

## Parameters / Member Variables
- : BufferAccessStrategy object configured for bulk write operations to optimize buffer replacement
- : Buffer identifier for the page currently being used for insertions
- : Block number of the next available free page from a previous bulk extension
- : Block number of the last free page available from bulk extension
- : Counter tracking how many pages this bulk insert operation has extended the relation by

## Dependencies
- Functions called/Symbols referenced:
  - [BulkInsertStateData](BulkInsertStateData.md)
  - [BufferAccessStrategy](BufferAccessStrategy.md)
  - Buffer
  - BlockNumber
- Called from (representative examples):
  - [GetBulkInsertState](../G/GetBulkInsertState.md)
  - [FreeBulkInsertState](../F/FreeBulkInsertState.md)
  - [ReleaseBulkInsertStatePin](../R/ReleaseBulkInsertStatePin.md)
  - [heap_insert](../h/heap_insert.md)
  - [heap_multi_insert](../h/heap_multi_insert.md)
  - [CopyFrom](../C/CopyFrom.md)
  - [ATRewriteTable](../A/ATRewriteTable.md)

## Notes and Other Information
- The structure is private to heapam.c and hio.c modules
- When current_buf is not InvalidBuffer, an extra pin is held on that buffer
- The bulk extension mechanism becomes more aggressive after extending by a significant number of pages
- Used primarily in COPY operations, table rewrites, and other bulk data loading scenarios
- Provides significant performance improvements for large data loads by reducing buffer management overhead