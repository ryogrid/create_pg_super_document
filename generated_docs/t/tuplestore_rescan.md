# tuplestore_rescan

## Location
src/backend/utils/sort/tuplestore.c: 1233 - 1267

## Overview
Rewinds the active read pointer of a tuplestore back to the beginning, enabling re-reading of the stored tuples from the start.

## Definition
```c
void tuplestore_rescan(Tuplestorestate *state)
```

## Detailed Description
This function resets the active read pointer of a tuplestore to its initial position, allowing the tuples to be read again from the beginning. The implementation varies depending on the current state of the tuplestore - whether it's entirely in memory (TSS_INMEM), writing to file (TSS_WRITEFILE), or reading from file (TSS_READFILE).

For in-memory tuplestores, it simply resets the current position counter to 0. For file-based operations, it resets the file position and offset to the beginning. For read-file state, it performs an actual file seek operation to reposition the file pointer to the start.

The function includes important assertions to ensure that the read pointer has the EXEC_FLAG_REWIND capability and that the tuplestore hasn't been truncated, as these conditions are prerequisites for rescan operations.

## Parameters / Member Variables
- `state`: Pointer to the Tuplestorestate structure representing the tuplestore to rescan

## Dependencies
- Functions called/Symbols referenced:
  - BufFileSeek (for file-based tuplestores)
  - [errcode_for_file_access](../e/errcode_for_file_access.md) (for error reporting)
  - ereport/errmsg (for error handling)
- Types referenced:
  - TSReadPointer
  - EXEC_FLAG_REWIND
  - TSS_INMEM, TSS_WRITEFILE, TSS_READFILE
- Called from (representative examples):
  - [ExecReScanMaterial](../E/ExecReScanMaterial.md)
  - [ExecReScanCteScan](../E/ExecReScanCteScan.md)
  - [ExecReScanFunctionScan](../E/ExecReScanFunctionScan.md)
  - [DoPortalRewind](../D/DoPortalRewind.md)
  - [PersistHoldablePortal](../P/PersistHoldablePortal.md)

## Notes and Other Information
- Requires that the read pointer was created with EXEC_FLAG_REWIND capability
- Cannot be used on truncated tuplestores (assertion enforced)
- Clears the eof_reached flag to allow reading from the beginning
- Handles different tuplestore states appropriately (in-memory vs file-based)
- Critical for implementing rescan operations in PostgreSQL executor nodes
- Used extensively in material nodes, CTE scans, and function scans that need to be re-executed
- Essential for portal operations that need to rewind and re-read results