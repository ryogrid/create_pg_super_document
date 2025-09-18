# ExecReScanSeqScan

## Location
[src/backend/executor/nodeSeqscan.c:212-237](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSeqscan.c#L212-L237)

## Overview
ExecReScanSeqScan is a function that rescans a sequential scan relation, allowing the scan to restart from the beginning of the table.

## Definition
```c
void ExecReScanSeqScan(SeqScanState *node)
```

## Detailed Description
This function implements the rescan functionality for sequential table scans in PostgreSQL's executor. It resets the current scan position to the beginning of the table, allowing the scan to be restarted. The function first checks if there is an active scan descriptor, and if so, calls table_rescan() to reset the scan position. It then calls ExecScanReScan() to handle common rescan operations for all scan types.

## Parameters / Member Variables
- `node`: A pointer to the SeqScanState structure containing the state information for the sequential scan operation

## Dependencies
- Functions called/Symbols referenced:
  - [table_rescan](../t/table_rescan.md)
  - [ExecScanReScan](ExecScanReScan.md)
- Types referenced:
  - [SeqScanState](../S/SeqScanState.md)
  - [TableScanDesc](../T/TableScanDesc.md)
  - [ScanState](../S/ScanState.md)
- Called from (representative examples):
  - [ExecReScan](ExecReScan.md) (in execAmi.c)

## Notes and Other Information
- This function is part of PostgreSQL's executor join support infrastructure
- It handles the case where the scan descriptor might be NULL (no active scan)
- The function delegates common rescan operations to ExecScanReScan after handling table-specific rescan logic
- Located in src/backend/executor/nodeSeqscan.c at lines 212-237