# ExecEndFunctionScan

## Location
[src/backend/executor/nodeFunctionscan.c:530-555](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeFunctionscan.c#L530-L555)

## Overview
ExecEndFunctionScan performs cleanup for function scan nodes by releasing tuplestores and other allocated resources.

## Definition


## Detailed Description
ExecEndFunctionScan handles the cleanup phase of function scan execution by systematically releasing resources allocated during the scan operation:

1. **Tuplestore Cleanup**: Iterates through all function states and calls tuplestore_end() on any active tuplestores, which:
   - Frees memory used by cached tuples
   - Releases temporary files if the tuplestore spilled to disk
   - Cleans up internal data structures

2. **State Reset**: Sets tuplestore pointers to NULL after cleanup to prevent accidental reuse.

3. **Resource Management**: Ensures proper cleanup of per-function resources without affecting shared executor state that may still be needed.

The function follows PostgreSQL's standard cleanup pattern where each node type is responsible for cleaning up its own specific resources, while the executor framework handles common cleanup tasks.

## Parameters / Member Variables
- : FunctionScanState containing the per-function states and tuplestores to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - [tuplestore_end](../t/tuplestore_end.md)
- Called from (representative examples):
  - [ExecEndNode](ExecEndNode.md)

## Notes and Other Information
- Only cleans up tuplestores, leaving other cleanup to the executor framework
- Safe to call multiple times due to NULL pointer checks
- Complements ExecReScanFunctionScan which resets tuplestores for reuse
- Part of PostgreSQL's standard three-phase executor lifecycle (Init, Execute, End)
- Does not free the FunctionScanState itself - that's handled by the parent context cleanup