# show_foreignscan_info

## Location
[src/backend/commands/explain.c:3651-3678](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L3651-L3678)

## Overview
Delegates to foreign data wrapper (FDW) routines to display additional explain information specific to ForeignScan nodes.

## Definition
```c
static void show_foreignscan_info(ForeignScanState *fsstate, ExplainState *es)
```

## Detailed Description
This function serves as a dispatch mechanism that allows Foreign Data Wrapper (FDW) implementations to provide custom explain output for their foreign scan operations. It determines whether the foreign scan is a SELECT operation or a direct modification operation (INSERT, UPDATE, DELETE) and calls the appropriate FDW-specific explain function.

For SELECT operations, it calls the FDW's ExplainForeignScan routine. For modification operations, it calls ExplainDirectModify. This design allows each FDW to provide detailed, implementation-specific information about how the foreign operation is being executed, such as remote SQL being generated, connection details, or optimization decisions.

## Parameters / Member Variables
- `fsstate`: ForeignScanState containing the execution state and FDW routine pointers for a foreign scan operation
- `es`: ExplainState containing output formatting information and the destination for explain output

## Dependencies
- Functions called/Symbols referenced:
  - [FdwRoutine](../F/FdwRoutine.md) (via fsstate->fdwroutine)
  - ExplainDirectModify (FDW callback function)
  - ExplainForeignScan (FDW callback function)
  - CMD_SELECT
  - [ForeignScan](../F/ForeignScan.md)
- Called from (representative examples):
  - [ExplainNode](../E/ExplainNode.md)

## Notes and Other Information
- The function only calls FDW explain routines if they are implemented (non-NULL function pointers)
- Different FDW implementations can provide vastly different explain output depending on their specific functionality
- This extensibility mechanism allows PostgreSQL to support various foreign data sources while providing meaningful explain information
- The function distinguishes between read operations (SELECT) and write operations (direct modify) to call the appropriate FDW explain routine