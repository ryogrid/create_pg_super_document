# table_scan_analyze_next_block

## Location
[src/include/access/tableam.h:1723-1738](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L1723-L1738)

## Overview
Prepares to analyze the next block in a read stream during table sampling operations, determining if the block is suitable for analysis.

## Definition


## Detailed Description
This function is part of PostgreSQL's table access method (tableam) interface for statistical analysis operations. It serves as a wrapper that delegates to the table access method's specific implementation of block preparation for analysis. The function is used during ANALYZE operations to sequentially process blocks in a table for statistical sampling.

The function may acquire resources such as locks that are held until the corresponding  operation completes. This ensures consistency during the sampling process.

## Parameters / Member Variables
- : TableScanDesc - A table scan descriptor that was initialized with 
- : ReadStream - The read stream containing the blocks to be analyzed

## Dependencies
- Functions called/Symbols referenced:
  - scan->rs_rd->rd_tableam->scan_analyze_next_block (table access method implementation)
- Types referenced:
  - [TableScanDesc](../T/TableScanDesc.md)
  - [ReadStream](../R/ReadStream.md)
- Called from (representative examples):
  - [acquire_sample_rows](../a/acquire_sample_rows.md) (src/backend/commands/analyze.c:1208)

## Notes and Other Information
- Returns false if the block is unsuitable for sampling, true otherwise
- Must be used with scans initiated by 
- Part of the table access method abstraction layer
- Resources acquired by this function are released when  returns false
- This is an inline function that provides a consistent interface across different table access methods