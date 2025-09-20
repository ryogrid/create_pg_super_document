# table_beginscan_analyze

## Location
[src/include/access/tableam.h:1009-1019](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L1009-L1019)

## Overview
Initializes a table scan descriptor specifically designed for ANALYZE operations, providing an alternative entry point that sets up scanning parameters optimized for statistical analysis.

## Definition

```c
static inline TableScanDesc
table_beginscan_analyze(Relation rel)
```
## Detailed Description
The  function creates a specialized table scan descriptor for ANALYZE commands. Unlike regular table scans, ANALYZE scans require different behavior and optimizations since they're used for collecting statistical information about table data rather than retrieving specific rows for query processing. This function leverages the same TableScanDesc data structure used by other scan types but configures it with the SO_TYPE_ANALYZE flag to indicate its special purpose.

The function acts as a wrapper around the table access method's scan_begin function, passing appropriate parameters for ANALYZE operations. It sets up the scan with no snapshot (NULL), no keys, and the SO_TYPE_ANALYZE flag to distinguish it from other scan types.

## Parameters / Member Variables
- : The relation (table) to be scanned for analysis purposes

## Dependencies
- Functions called/Symbols referenced:
  - SO_TYPE_ANALYZE (scan option flag)
  - rel->rd_tableam->scan_begin (table access method scan initialization)
- Called from (representative examples):
  - [acquire_sample_rows](../a/acquire_sample_rows.md) (src/backend/commands/analyze.c:1196)

## Notes and Other Information
- This is an inline function defined in the table access method header file
- The function uses the SO_TYPE_ANALYZE flag to signal that this scan is for statistical analysis
- ANALYZE scans have different performance characteristics and may use sampling techniques
- The scan is initialized without a snapshot or key constraints since ANALYZE typically samples the entire table
- Part of PostgreSQL's table access method (TAM) abstraction layer