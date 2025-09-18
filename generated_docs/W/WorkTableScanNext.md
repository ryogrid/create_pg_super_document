# WorkTableScanNext

## Location
[src/backend/executor/nodeWorktablescan.c:30-65](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeWorktablescan.c#L30-L65)

## Overview
WorkTableScanNext is the core function that retrieves the next tuple from a worktable during a recursive query execution, serving as the main workhorse for ExecWorkTableScan.

## Definition
static TupleTableSlot *WorkTableScanNext(WorkTableScanState *node)

## Detailed Description
WorkTableScanNext implements the tuple retrieval mechanism for worktable scans, which are used in PostgreSQL's recursive query processing. The function fetches tuples from a tuplestore that serves as the temporary working table for recursive operations. It is specifically optimized for forward-only scanning and assumes it is the only reader of the worktable, eliminating the need for private read pointers or tuple copying. The function intentionally does not support backward scanning to avoid performance overhead in the tuplestore creation, as backward scans are never useful for worktable nodes in practice.

## Parameters / Member Variables
- `node`: WorkTableScanState pointer containing the scan state information, including access to the recursive union state and scan tuple slot

## Dependencies
- Functions called/Symbols referenced:
  - ScanDirectionIsForward (assertion check for forward scan direction)
  - [tuplestore_gettupleslot](../t/tuplestore_gettupleslot.md) (retrieves next tuple from tuplestore)
- Types used:
  - [WorkTableScanState](WorkTableScanState.md) (scan state structure)
  - TuplestoreState (tuplestore for temporary data)
  - TupleTableSlot (tuple storage slot)
- Called from:
  - [ExecWorkTableScan](../E/ExecWorkTableScan.md) (main execution function)

## Notes and Other Information
- Only supports forward scanning for performance reasons
- Assumes single reader access to the worktable tuplestore
- No tuple copying is needed due to exclusive access assumption
- Cannot appear high enough in plan trees to require backward scan support
- Returns NULL when no more tuples are available
- Uses the scan tuple slot from the scan state for tuple storage