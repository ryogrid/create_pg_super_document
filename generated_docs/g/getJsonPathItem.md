# getJsonPathItem

## Location
[src/backend/utils/adt/jsonpath_exec.c:2958-2990](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L2958-L2990)

## Overview
Converts a jsonpath scalar or variable node to an actual JsonbValue, handling different data types including null, boolean, numeric, string, and variable references.

## Definition

```c
static void
getJsonPathItem(JsonPathExecContext *cxt, JsonPathItem *item,
				JsonbValue *value)
```
## Detailed Description
This function serves as a converter that transforms jsonpath items (nodes in the jsonpath expression tree) into concrete JsonbValue structures that can be used in JSON operations. It handles five main jsonpath item types: null literals, boolean literals, numeric literals, string literals, and variable references. For the first four types, it directly converts the values using appropriate accessor functions. For variables, it delegates to getJsonPathVariable to resolve the variable value from the execution context.

## Parameters / Member Variables
- : JsonPathExecContext pointer providing the execution context, particularly needed for variable resolution
- : JsonPathItem pointer representing the jsonpath node to convert
- : JsonbValue pointer where the converted value will be stored (output parameter)

## Dependencies
- Functions called/Symbols referenced:
  - [jspGetBool](../j/jspGetBool.md) (extracts boolean value from jsonpath item)
  - [jspGetNumeric](../j/jspGetNumeric.md) (extracts numeric value from jsonpath item)  
  - [jspGetString](../j/jspGetString.md) (extracts string value and length from jsonpath item)
  - [getJsonPathVariable](getJsonPathVariable.md) (resolves variable references)
- Data types used:
  - [JsonPathExecContext](../J/JsonPathExecContext.md), JsonPathItem, JsonbValue
  - jsonpath item types: jpiNull, jpiBool, jpiNumeric, jpiString, jpiVariable
  - JSON value types: jbvNull, jbvBool, jbvNumeric, jbvString
- Called from (representative examples):
  - RETURN_ERROR macro in jsonpath_exec.c:310
  - [executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md) in jsonpath_exec.c:782

## Notes and Other Information
- This is a static function internal to the jsonpath execution module
- The function modifies the output parameter 'value' rather than returning a value
- [Variable](../V/Variable.md) processing is handled specially - it calls getJsonPathVariable and returns early
- Uses elog(ERROR) for unexpected jsonpath item types, which will terminate execution
- Part of the PostgreSQL jsonpath execution engine's type conversion system