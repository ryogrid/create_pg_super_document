# ExecInitScanTupleSlot

## Location
src/backend/executor/execTuples.c: 1898 - 1917

## Overview
Initializes the scan tuple slot for a scan node, allocating a new TupleTableSlot with a specified tuple descriptor and configuring scan-related state information.

## Definition
```c
void ExecInitScanTupleSlot(EState *estate, ScanState *scanstate, TupleDesc tupledesc, const TupleTableSlotOps *tts_ops)
```

## Detailed Description
ExecInitScanTupleSlot is specifically designed for scan nodes in PostgreSQL's executor. It allocates a scan tuple slot using the provided tuple descriptor and associates it with the scan state. The function also sets up various scan-related metadata in the ScanState, including the scan descriptor and operation flags. This function is essential for scan nodes that read tuples from tables, indexes, or other data sources, ensuring they have the proper tuple slot infrastructure to hold and manipulate the scanned tuples.

## Parameters / Member Variables
- `estate`: Pointer to the execution state containing the tuple table
- `scanstate`: Pointer to the ScanState structure that needs its scan slot initialized
- `tupledesc`: Tuple descriptor defining the structure of tuples to be scanned
- `tts_ops`: Pointer to TupleTableSlotOps structure defining the operations for the tuple slot

## Dependencies
- Functions called/Symbols referenced:
  - ExecAllocTableSlot: Creates and adds the tuple slot to the tuple table
  - ScanState: Structure containing scan-specific state information
  - TupleTableSlotOps: Structure defining slot operations
- Called from (representative examples):
  - ExecInitSeqScan: Sequential scan initialization
  - ExecInitIndexScan: Index scan initialization
  - ExecInitBitmapHeapScan: Bitmap heap scan initialization
  - ExecInitForeignScan: Foreign scan initialization
  - ExecInitSubqueryScan: Subquery scan initialization
  - And many other scan node initialization functions

## Notes and Other Information
- Specifically designed for scan nodes that read from data sources
- Sets up both the slot and scan descriptor in one operation
- The scan descriptor is stored in both ss_ScanTupleSlot and ps.scandesc fields
- Manages scan operation flags (scanopsfixed, scanopsset) for proper state tracking
- Used across all types of scan operations including table scans, index scans, and function scans
- Located in src/backend/executor/execTuples.c:1898-1917