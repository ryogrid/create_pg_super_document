# ExecInitTableFuncScan

## Location
[src/backend/executor/nodeTableFuncscan.c:111-219](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeTableFuncscan.c#L111-L219)

## Overview
ExecInitTableFuncScan initializes a TableFuncScanState node for executing table function scans, setting up all necessary data structures, expression contexts, and type conversion information.

## Definition


## Detailed Description
ExecInitTableFuncScan performs comprehensive initialization of a table function scan node. It creates a TableFuncScanState structure and initializes all components needed for table function execution, including expression contexts, tuple descriptors, projection information, and function-specific routines.

The function supports both XMLTABLE and JSON_TABLE operations by selecting the appropriate routine based on the function type. It builds a tuple descriptor from the column specifications, initializes all expressions (document, row, column, and passing expressions), and sets up type input functions for data conversion from text to the target column types.

Key initialization steps include creating a per-table memory context, setting up namespace URIs, and preparing function manager info for efficient type conversions during execution.

## Parameters / Member Variables
- : TableFuncScan plan node containing the table function specification
- : Execution state providing the execution environment and memory context
- : Execution flags, with EXEC_FLAG_MARK being explicitly unsupported

## Dependencies
- Functions called/Symbols referenced:
  - TableFuncScan, TableFuncScanState, TableFunc (struct types)
  - [ExecTableFuncScan](ExecTableFuncScan.md) (assigned as execution function)
  - ExecAssignExprContext, ExecInitScanTupleSlot, ExecInitResultTypeTL
  - [BuildDescFromLists](../B/BuildDescFromLists.md), ExecAssignScanProjectionInfo, ExecInitQual
  - [ExecInitExpr](ExecInitExpr.md), ExecInitExprList (expression initialization)
  - AllocSetContextCreate, getTypeInputInfo, fmgr_info
- Called from:
  - [ExecInitNode](ExecInitNode.md) (main executor initialization)
  - Referenced in nodeTableFuncscan.h header

## Notes and Other Information
- Supports only XMLTABLE and JSON_TABLE function types currently
- Creates a dedicated memory context for per-table operations
- Asserts that table function scans have no child plans (outer/inner)
- Does not support the EXEC_FLAG_MARK execution flag
- Initializes type input functions for all output columns to enable text-to-type conversion
- Sets up separate expression lists for different components (columns, defaults, values, passing parameters)