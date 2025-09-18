# NamedTuplestoreScanState

## Location
src/include/nodes/execnodes.h: 2010 - 2016

## Overview
NamedTuplestoreScanState is the execution state node for scanning pre-existing named tuplestores in PostgreSQL. It is commonly used for accessing transition tables in AFTER triggers and other scenarios where data is stored in a named tuplestore prior to query execution.

## Definition
```c
typedef struct NamedTuplestoreScanState
{
    ScanState       ss;         /* its first field is NodeTag */
    int             readptr;    /* index of my tuplestore read pointer */
    TupleDesc       tupdesc;    /* format of the tuples in the tuplestore */
    Tuplestorestate *relation;  /* the rows */
} NamedTuplestoreScanState;
```

## Detailed Description
NamedTuplestoreScanState provides the execution state for scanning tuplestores that have been created and named before the query execution begins. This is particularly useful for AFTER triggers where transition tables (OLD and NEW) are created as named tuplestores that can be referenced within the trigger function. Multiple NamedTuplestoreScan nodes can read from the same tuplestore concurrently, each maintaining their own read pointer to track their position independently.

## Parameters / Member Variables
- `ss`: Base ScanState structure containing common scan node fields and NodeTag
- `readptr`: Index of this specific scan node tuplestore read pointer for position tracking
- `tupdesc`: Tuple descriptor defining the format and structure of tuples stored in the tuplestore
- `relation`: Pointer to the Tuplestorestate containing the actual row data to be scanned

## Dependencies
- Functions called/Symbols referenced:
  - ScanState
  - TupleDesc
  - Tuplestorestate
- Called from (representative examples):
  - ExecNamedTuplestoreScan
  - ExecInitNamedTuplestoreScan
  - ExecReScanNamedTuplestoreScan
  - NamedTuplestoreScanNext
  - NamedTuplestoreScanRecheck

## Notes and Other Information
- Primarily used for AFTER trigger transition tables where OLD and NEW row sets are pre-stored
- Multiple scan nodes can read from the same named tuplestore without interference
- The tuplestore and its format are established before query execution begins
- Each scan node maintains its own read pointer allowing independent traversal of the data
- The tupdesc field ensures proper interpretation of the stored tuple format during scanning