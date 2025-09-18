# getJsonPathVariable

## Location
src/backend/utils/adt/jsonpath_exec.c: 3141 - 3172

## Overview
Retrieves the value of a variable passed to the JSON path executor by name and sets up the execution context appropriately.

## Definition
static void getJsonPathVariable(JsonPathExecContext *cxt, JsonPathItem *variable, JsonbValue *value)

## Detailed Description
This static function is responsible for resolving variable references during JSON path execution. It extracts the variable name from the JsonPathItem, looks up the variable value in the execution context's variable store, and handles base object setup if needed. If the variable is not found, it reports an error with details about the missing variable name. This function is essential for supporting parameterized JSON path queries where variables can be passed in and referenced within the path expression.

## Parameters / Member Variables
- cxt: Pointer to the JSON path execution context containing variable storage and lookup functions
- variable: Pointer to the JsonPathItem representing the variable reference (must be of type jpiVariable)
- value: Pointer to JsonbValue where the resolved variable value will be stored

## Dependencies
- Functions called/Symbols referenced:
  - jspGetString (extracts string data from JsonPathItem)
  - pnstrdup (creates null-terminated string copy)
  - setBaseObject (sets up base object context)
  - ereport (PostgreSQL error reporting)
  - jpiVariable (JsonPathItem type constant)
- Called from (representative examples):
  - getJsonPathItem (main JSON path item processing function)

## Notes and Other Information
- This is a static helper function, only accessible within the jsonpath_exec.c module
- Requires that the JsonPathItem is of type jpiVariable (verified by assertion)
- Uses the execution context's getVar function pointer for actual variable lookup
- Supports base object functionality for complex variable scenarios
- Will throw an ERRCODE_UNDEFINED_OBJECT error if the variable is not found
- Part of PostgreSQL's JSON path variable resolution infrastructure