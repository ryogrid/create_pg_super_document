# appendBoolResult

## Location
src/backend/utils/adt/jsonpath_exec.c: 2930 - 2957

## Overview
Converts a boolean execution status to a JSON boolean item and continues jsonpath execution with the next jsonpath item.

## Definition


## Detailed Description
This function is responsible for converting a boolean execution result from jsonpath operations into a proper JSON boolean value and continuing the execution chain. It handles the conversion of JsonPathBool values (jpbTrue, jpbFalse, jpbUnknown) to appropriate JsonbValue representations. When the result is jpbUnknown, it converts to a JSON null value, otherwise it creates a JSON boolean. The function then delegates to executeNextItem to continue processing the jsonpath expression.

## Parameters / Member Variables
- : JsonPathExecContext pointer containing the execution context for the jsonpath operation
- : JsonPathItem pointer representing the current jsonpath item being processed
- : JsonValueList pointer to store found values, can be NULL for singleton operations
- : JsonPathBool enumeration value representing the boolean result (jpbTrue, jpbFalse, or jpbUnknown)

## Dependencies
- Functions called/Symbols referenced:
  - [jspGetNext](../j/jspGetNext.md) (gets the next jsonpath item)
  - [executeNextItem](../e/executeNextItem.md) (continues execution with the next item)
- Data types used:
  - [JsonPathExecContext](../J/JsonPathExecContext.md), JsonPathItem, JsonValueList, JsonPathBool
  - [JsonbValue](../J/JsonbValue.md) (for creating JSON values)
  - jperOk (return status for successful singleton boolean)
  - jbvNull, jbvBool (JSON value types)
  - jpbUnknown, jpbTrue (boolean result values)
- Called from (representative examples):
  - RETURN_ERROR macro in jsonpath_exec.c:308
  - [executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md) in jsonpath_exec.c:807

## Notes and Other Information
- This is a static function internal to the jsonpath execution module
- Handles the special case where jpbUnknown is converted to JSON null rather than a boolean
- Returns jperOk immediately for singleton boolean values when there's no next item and no found list
- Part of the PostgreSQL jsonpath execution engine that processes SQL/JSON path expressions