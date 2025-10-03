# TableFuncNext

## Location
[src/backend/executor/nodeTableFuncscan.c:54-80](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeTableFuncscan.c#L54-L80)

## Overview
TableFuncNext is a static helper function that serves as the core tuple retrieval mechanism for table function scans, fetching tuples from a tuplestore for subsequent processing.

## Definition

```c
static TupleTableSlot *
TableFuncNext(TableFuncScanState *node)
```
## Detailed Description
TableFuncNext implements a lazy evaluation strategy for table function execution. On the first call, it triggers the execution of the entire table function via tfuncFetchRows(), storing all resulting tuples in a tuplestore. Subsequent calls simply retrieve the next tuple from this pre-populated tuplestore. This approach ensures that the table function is executed only once while allowing efficient sequential access to its results.

The function operates as a workhorse for ExecTableFuncScan, handling the low-level mechanics of tuple retrieval and maintaining the scan state across multiple calls.

## Parameters / Member Variables
- : TableFuncScanState structure containing the scan state, including the tuplestore and scan tuple slot

## Dependencies
- Functions called/Symbols referenced:
  - [TableFuncScanState](TableFuncScanState.md) (struct type)
  - [tfuncFetchRows](../t/tfuncFetchRows.md) (function to fetch all rows from table function)
  - [tuplestore_gettupleslot](../t/tuplestore_gettupleslot.md) (function to retrieve next tuple from tuplestore)
- Called from:
  - [ExecTableFuncScan](../E/ExecTableFuncScan.md)

## Notes and Other Information
- Uses a lazy initialization pattern - the tuplestore is only populated on first access
- The function is static, indicating it's only used within the nodeTableFuncscan.c file
- Implements a pull-based model where tuples are retrieved on demand from a pre-populated store
- The tuplestore approach allows for potential rewind operations and multiple scans of the same result set

## Simplified Source

```c
static TupleTableSlot *
TableFuncNext(TableFuncScanState *node)
{
    TupleTableSlot *scanslot = node->ss.ss_ScanTupleSlot;

    // First call: execute table function and store all tuples
    if (node->tupstore == NULL)
        tfuncFetchRows(node, node->ss.ps.ps_ExprContext);

    // Get next tuple from tuplestore
    tuplestore_gettupleslot(node->tupstore, true, false, scanslot);
    return scanslot;
}
```