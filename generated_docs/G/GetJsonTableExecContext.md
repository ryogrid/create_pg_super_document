# GetJsonTableExecContext

## Location
[src/backend/utils/adt/jsonpath_exec.c:4090-4110](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L4090-L4110)

## Overview
A static inline validation function that safely extracts and validates the JsonTableExecContext from a TableFuncScanState structure with comprehensive error checking.

## Definition
```c
static inline JsonTableExecContext *GetJsonTableExecContext(TableFuncScanState *state, 
                                                           const char *fname)
```

## Detailed Description
The `GetJsonTableExecContext` function serves as a critical validation and extraction utility for JSON_TABLE operations in PostgreSQL. It performs two levels of safety checks: first verifying that the provided state parameter is indeed a valid TableFuncScanState structure using PostgreSQL's IsA() macro, then validating that the opaque pointer contains a properly initialized JsonTableExecContext by checking its magic number. This dual validation approach ensures type safety and prevents crashes from corrupted or incorrectly initialized execution contexts. The function is designed to be called from various JSON_TABLE-related functions, providing consistent error reporting with the caller's function name.

## Parameters / Member Variables
- `state`: Pointer to the TableFuncScanState structure containing the execution context
- `fname`: Name of the calling function, used for error reporting and debugging

## Dependencies
- Functions called/Symbols referenced:
  - IsA (PostgreSQL type checking macro)
  - elog (error logging function)
  - JSON_TABLE_EXEC_CONTEXT_MAGIC (validation constant)
  - [TableFuncScanState](../T/TableFuncScanState.md) (type)
  - [JsonTableExecContext](../J/JsonTableExecContext.md) (type)
- Called from (representative examples):
  - [JsonTableDestroyOpaque](../J/JsonTableDestroyOpaque.md)
  - [JsonTableSetDocument](../J/JsonTableSetDocument.md)
  - [JsonTableFetchRow](../J/JsonTableFetchRow.md)
  - [JsonTableGetValue](../J/JsonTableGetValue.md)

## Notes and Other Information
- Returns a validated JsonTableExecContext pointer on success
- Throws ERROR-level exceptions on validation failures, preventing further execution
- Uses magic number validation pattern common in PostgreSQL for structure integrity checking
- The inline qualifier suggests this is a performance-critical validation function
- Part of the JSON_TABLE functionality which allows querying JSON data as relational tables
- Provides consistent error messages that include the calling function name for better debugging
- Essential for maintaining the integrity of JSON_TABLE execution state across complex operations