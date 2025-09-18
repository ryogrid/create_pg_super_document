# JsonTableSetDocument

## Location
src/backend/utils/adt/jsonpath_exec.c: 4240 - 4252

## Overview
Installs a new input JSON document for JSON_TABLE processing and initiates row pattern evaluation by resetting the root plan state.

## Definition
```c
static void JsonTableSetDocument(TableFuncScanState *state, Datum value)
```

## Detailed Description
JsonTableSetDocument is a static function that serves as the entry point for processing a new JSON document in JSON_TABLE operations. The function performs two main tasks:

1. **Context Retrieval**: Obtains the JsonTableExecContext from the TableFuncScanState, validating that the context is properly initialized and has the correct magic number.

2. **Pattern Reset**: Calls JsonTableResetRowPattern on the root plan state with the new document value, which initiates the row pattern evaluation process for the entire JSON_TABLE plan hierarchy.

This function effectively starts a new document processing cycle, resetting all internal state and beginning pattern matching against the provided JSON document.

## Parameters / Member Variables
- `state`: TableFuncScanState pointer containing the scan state and execution context
- `value`: Datum containing the JSON document to be processed

## Dependencies
- Functions called/Symbols referenced:
  - GetJsonTableExecContext (context retrieval and validation)
  - JsonTableResetRowPattern (row pattern reset and evaluation)
  - JsonTableExecContext (struct type)
- Called from (representative examples):
  - Table function scan execution routines
  - JSON_TABLE document iteration logic

## Notes and Other Information
- This is a static function within jsonpath_exec.c, indicating it's internal to JSON path execution
- The function acts as a bridge between the table function infrastructure and JSON_TABLE-specific processing
- It assumes the execution context has been properly initialized via JsonTableInitOpaque
- The function is typically called once per input JSON document in a JSON_TABLE scan
- Error handling relies on GetJsonTableExecContext to validate the context state
- The actual work of pattern evaluation is delegated to JsonTableResetRowPattern, keeping this function focused on document setup