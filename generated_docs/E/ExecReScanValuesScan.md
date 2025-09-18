# ExecReScanValuesScan

## Location
[src/backend/executor/nodeValuesscan.c:328-336](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeValuesscan.c#L328-L336)

## Overview
ExecReScanValuesScan resets a VALUES scan to begin scanning from the start, clearing any cached results and resetting the current position index.

## Definition
```c
void ExecReScanValuesScan(ValuesScanState *node)
```

## Detailed Description
ExecReScanValuesScan implements the rescan functionality for VALUES scan nodes, which is required when the scan needs to be restarted (for example, in nested loop joins where the inner scan must be repeated for each outer tuple). The function performs three key operations: clears any cached result tuple from the result slot, calls the generic ExecScanReScan to handle common scan rescan operations, and resets the current row index to -1 to indicate that no row is currently selected.

This function ensures that subsequent calls to the scan will start from the beginning of the VALUES list, providing the correct behavior for operations that require multiple passes through the data.

## Parameters / Member Variables
- `node`: ValuesScanState containing the scan state to be reset, including the current position index and scan infrastructure

## Dependencies
- Functions called/Symbols referenced:
  - ExecClearTuple
  - [ExecScanReScan](ExecScanReScan.md)
- Called from:
  - [ExecReScan](ExecReScan.md)

## Notes and Other Information
- Resets curr_idx to -1 to indicate the scan should start from the beginning
- Clears the result tuple slot if it exists to avoid stale data
- Delegates common rescan operations to ExecScanReScan for consistency with other scan types
- Essential for correct behavior in nested loop joins and other operations requiring multiple scan passes
- Part of the standard executor interface for scan node operations