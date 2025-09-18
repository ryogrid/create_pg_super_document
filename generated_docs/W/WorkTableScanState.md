# WorkTableScanState

## Location
src/include/nodes/execnodes.h: 2026 - 2030

## Overview
WorkTableScanState is the execution state node for scanning work tables created by RecursiveUnion nodes in PostgreSQL. It is used specifically in recursive query execution to access the working data produced during recursive iterations.

## Definition
```c
typedef struct WorkTableScanState
{
    ScanState           ss;        /* its first field is NodeTag */
    RecursiveUnionState *rustate;
} WorkTableScanState;
```

## Detailed Description
WorkTableScanState provides the execution state for scanning work tables that are created and managed by RecursiveUnion nodes during recursive query execution. In recursive Common Table Expressions (CTEs), the RecursiveUnion node maintains a work table containing the results of each recursive iteration. The WorkTableScanState locates and connects to the corresponding RecursiveUnion node during executor startup to access this dynamically created work table data.

## Parameters / Member Variables
- `ss`: Base ScanState structure containing common scan node fields and NodeTag
- `rustate`: Pointer to the RecursiveUnionState that owns and manages the work table being scanned

## Dependencies
- Functions called/Symbols referenced:
  - [ScanState](../S/ScanState.md)
  - [RecursiveUnionState](../R/RecursiveUnionState.md)
- Called from (representative examples):
  - [ExecWorkTableScan](../E/ExecWorkTableScan.md)
  - [ExecInitWorkTableScan](../E/ExecInitWorkTableScan.md)
  - [ExecReScanWorkTableScan](../E/ExecReScanWorkTableScan.md)
  - [WorkTableScanNext](WorkTableScanNext.md)
  - [WorkTableScanRecheck](WorkTableScanRecheck.md)

## Notes and Other Information
- Specifically designed for recursive query execution where work tables store intermediate results
- The RecursiveUnion node creates and manages the work table that this scan node accesses
- Connection to the RecursiveUnion node is established during executor startup phase
- Essential component in implementing recursive CTEs and other recursive query patterns
- The work table contents change dynamically as the recursive query progresses through iterations