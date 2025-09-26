# ExecOpenScanRelation

## Location
src/backend/executor/execUtils.c: 697 - 727

## Overview
Opens a heap relation for scanning by a base-level scan plan node, with validation to ensure the relation is scannable.

## Definition
```c
Relation ExecOpenScanRelation(EState *estate, Index scanrelid, int eflags)
```

## Detailed Description
ExecOpenScanRelation is a fundamental utility function that opens a relation for scanning during query execution. This function is typically called during a scan node's initialization routine (ExecInit) to prepare the relation for data access.

The function first retrieves the relation using ExecGetRangeTableRelation, then performs important validation to ensure the relation is actually scannable. Specifically, it checks for materialized views that have not been populated, which would result in an empty or invalid scan. This validation is skipped during EXPLAIN-only queries or when the WITH_NO_DATA flag is set.

If an unscannable materialized view is detected during actual execution, the function raises an error with a helpful hint to use REFRESH MATERIALIZED VIEW to populate the view before scanning.

## Parameters / Member Variables
- `estate`: Execution state containing range table and query context information
- `scanrelid`: Index of the relation in the range table to be opened for scanning
- `eflags`: Execution flags that control behavior (e.g., EXEC_FLAG_EXPLAIN_ONLY, EXEC_FLAG_WITH_NO_DATA)

## Dependencies
- Functions called/Symbols referenced:
  - ExecGetRangeTableRelation
  - RelationIsScannable
  - EXEC_FLAG_EXPLAIN_ONLY
  - EXEC_FLAG_WITH_NO_DATA
- Called from (representative examples):
  - ExecInitBitmapHeapScan
  - ExecInitCustomScan
  - ExecInitForeignScan
  - ExecInitIndexOnlyScan
  - ExecInitIndexScan
  - ExecInitSampleScan
  - ExecInitSeqScan
  - ExecInitTidRangeScan
  - ExecInitTidScan

## Notes and Other Information
- This function is called during the initialization phase of various scan node types
- Provides centralized validation for scannable relations, particularly materialized views
- The validation is bypassed for EXPLAIN operations and queries with NO_DATA flag
- Located in src/backend/executor/execUtils.c:697-727
- Returns the opened Relation object for use by the scan node
- Essential for ensuring data integrity by preventing scans of unpopulated materialized views
- The error message provides clear guidance on how to fix unpopulated materialized view issues