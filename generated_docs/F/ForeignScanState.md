# ForeignScanState

## Location
[src/include/nodes/execnodes.h:2038-2047](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L2038-L2047)

## Overview
ForeignScanState is the execution state node for scanning foreign-data tables in PostgreSQL. It provides the interface between the PostgreSQL executor and Foreign Data Wrapper (FDW) implementations for accessing external data sources.

## Definition
```c
typedef struct ForeignScanState
{
    ScanState       ss;                 /* its first field is NodeTag */
    ExprState      *fdw_recheck_quals;  /* original quals not in ss.ps.qual */
    Size            pscan_len;          /* size of parallel coordination information */
    ResultRelInfo  *resultRelInfo;      /* result rel info, if UPDATE or DELETE */
    /* use struct pointer to avoid including fdwapi.h here */
    struct FdwRoutine *fdwroutine;
    void           *fdw_state;          /* foreign-data wrapper can keep state here */
} ForeignScanState;
```

## Detailed Description
ForeignScanState serves as the execution state for foreign table scans, enabling PostgreSQL to access data from external sources through the Foreign Data Wrapper (FDW) interface. It maintains connections to FDW-specific routines and state, handles qualification expressions that need to be rechecked locally, and supports both parallel query execution and DML operations (UPDATE/DELETE) on foreign tables. The structure provides a bridge between PostgreSQL execution framework and various FDW implementations.

## Parameters / Member Variables
- `ss`: Base ScanState structure containing common scan node fields and NodeTag
- `fdw_recheck_quals`: Expression state for original qualification conditions that cannot be pushed down to the foreign source and must be rechecked locally
- `pscan_len`: Size of parallel coordination information for parallel query execution support
- `resultRelInfo`: Result relation information used when the scan is part of UPDATE or DELETE operations
- `fdwroutine`: Pointer to FDW-specific routine functions that implement the foreign data access methods
- `fdw_state`: Opaque pointer allowing the foreign data wrapper to maintain its own private state information

## Dependencies
- Functions called/Symbols referenced:
  - [ScanState](../S/ScanState.md)
  - ExprState
  - [ResultRelInfo](../R/ResultRelInfo.md)
  - [FdwRoutine](FdwRoutine.md)
- Called from (representative examples):
  - [ExecForeignScan](../E/ExecForeignScan.md)
  - [ExecInitForeignScan](../E/ExecInitForeignScan.md)
  - [ExecEndForeignScan](../E/ExecEndForeignScan.md)
  - [ExecReScanForeignScan](../E/ExecReScanForeignScan.md)
  - [ForeignNext](ForeignNext.md)
  - [ForeignRecheck](ForeignRecheck.md)
  - [ExecForeignScanEstimate](../E/ExecForeignScanEstimate.md)
  - [ExecAsyncForeignScanRequest](../E/ExecAsyncForeignScanRequest.md)

## Notes and Other Information
- Central component of PostgreSQL Foreign Data Wrapper architecture for external data access
- Supports both read operations (SELECT) and write operations (UPDATE/DELETE) on foreign tables
- Handles qualification pushdown optimization while maintaining local recheck capability
- Includes support for parallel query execution across foreign data sources
- The fdw_state field allows FDW implementations to maintain connection pools, cursors, and other state
- Works in conjunction with FdwRoutine function pointers to provide extensible foreign data access