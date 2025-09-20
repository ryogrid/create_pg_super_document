# xl_brin_createidx

## Location
[src/include/access/brin_xlog.h:50-54](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/brin_xlog.h#L50-L54)

## Overview
A WAL (Write-Ahead Logging) record structure that stores the necessary information for logging BRIN (Block Range Index) index creation operations.

## Definition

```c
typedef struct xl_brin_createidx
{
	BlockNumber pagesPerRange;
	uint16		version;
} xl_brin_createidx;
```
## Detailed Description
The  structure is used in PostgreSQL's WAL system to record the creation of a BRIN index. BRIN indexes are designed for very large tables where the data has some natural correlation with its physical location (such as time-series data). This WAL record captures the essential parameters needed to recreate or describe a BRIN index during recovery operations.

The structure stores two critical pieces of information: the pages-per-range parameter that determines the granularity of the BRIN index, and the version information for compatibility purposes. During WAL replay, this information is used along with backup block 0 (the metapage) to properly reconstruct the BRIN index state.

## Parameters / Member Variables
- : Specifies the number of table pages that each BRIN index entry summarizes. This determines the granularity of the index - larger values mean each index entry covers more pages but with potentially less precise selectivity
- : The version number of the BRIN index format, used for compatibility and upgrade purposes

## Dependencies
- Functions called/Symbols referenced:
  - BlockNumber (type)
  - uint16 (type)
- Called from (representative examples):
  - [brinbuild](../b/brinbuild.md) (in src/backend/access/brin/brin.c:1127)
  - [brin_xlog_createidx](../b/brin_xlog_createidx.md) (in src/backend/access/brin/brin_xlog.c:27)
  - [brin_desc](../b/brin_desc.md) (in src/backend/access/rmgrdesc/brindesc.c:28)
  - SizeOfBrinCreateIdx (macro in src/include/access/brin_xlog.h:55)

## Notes and Other Information
- This structure is specifically designed for WAL logging and recovery operations
- The structure is used in conjunction with backup block 0 which contains the metapage data
- The  macro is defined to calculate the size of this structure for WAL record operations
- BRIN indexes are particularly effective for large, naturally ordered datasets where traditional B-tree indexes would be too large