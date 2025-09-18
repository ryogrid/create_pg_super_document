# JsonTableFetchRow

## Location
src/backend/utils/adt/jsonpath_exec.c: 4438 - 4453

## Overview
JsonTableFetchRow serves as the main interface for advancing to the next row in JSON table processing, preparing the row context for subsequent column value extraction operations.

## Definition


## Detailed Description
This function acts as the primary entry point for row iteration in PostgreSQL's JSON table functionality. It retrieves the JsonTableExecContext from the provided TableFuncScanState and delegates the actual row fetching to the root plan's JsonTablePlanNextRow function. The function serves as a bridge between PostgreSQL's table function infrastructure and the JSON table-specific execution logic, maintaining the execution context and ensuring proper integration with the broader query execution framework.

## Parameters / Member Variables
- : Pointer to TableFuncScanState structure containing the scan state for the table function, which includes the JSON table execution context and other scan-related information

## Dependencies
- Functions called/Symbols referenced:
  - [GetJsonTableExecContext](../G/GetJsonTableExecContext.md) (retrieves the JSON table execution context from the scan state)
  - [JsonTablePlanNextRow](JsonTablePlanNextRow.md) (performs the actual row advancement using the root plan state)
- Called from (representative examples):
  - PostgreSQL table function infrastructure (as part of the table function scan operations)

## Notes and Other Information
- Returns true if a new row was successfully prepared, false if no more rows are available
- This function is the interface between PostgreSQL's table function framework and JSON table-specific execution logic
- The function name is passed to GetJsonTableExecContext for debugging/error reporting purposes
- After successful row fetching, subsequent JsonTableGetValue calls can extract individual column values from the prepared row context
- The function operates on the root plan state (cxt->rootplanstate), which may contain complex nested plan hierarchies
- This is typically called repeatedly by the PostgreSQL executor until it returns false, indicating the end of the result set