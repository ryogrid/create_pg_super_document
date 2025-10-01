# setBaseObject

## Location
[src/backend/utils/adt/jsonpath_exec.c:3494-3505](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L3494-L3505)

## Overview
Saves the current base object and its identifier in the JSONPath execution context, primarily needed for the execution of .keyvalue() operations.

## Definition
```c
static JsonBaseObjectInfo setBaseObject(JsonPathExecContext *cxt, JsonbValue *jbv, int32 id)
```

## Detailed Description
The setBaseObject function manages the base object context during JSONPath execution. It saves the current base object information from the execution context, then updates the context with a new base object and identifier. For binary JSON values, it extracts the JsonbContainer from the binary data; for other types, it sets the container pointer to NULL. This mechanism is essential for supporting JSONPath operations like .keyvalue() that need to reference the containing object.

## Parameters / Member Variables
- `cxt`: JSONPath execution context to be updated with new base object information
- `jbv`: JsonbValue representing the new base object to be set
- `id`: Integer identifier associated with the base object

## Dependencies
- Functions called/Symbols referenced:
  - [JsonPathExecContext](../J/JsonPathExecContext.md) (execution context structure)
  - [JsonBaseObjectInfo](../J/JsonBaseObjectInfo.md) (base object information structure)
  - jbvBinary (JSON binary value type constant)
  - [JsonbContainer](../J/JsonbContainer.md) (container structure for binary JSON data)
- Called from (representative examples):
  - [executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md) (during item unwrapping)
  - [executeKeyValueMethod](../e/executeKeyValueMethod.md) (for .keyvalue() operations)
  - [getJsonPathVariable](../g/getJsonPathVariable.md) (variable resolution)

## Notes and Other Information
- Returns the previous JsonBaseObjectInfo to enable restoration of the prior context
- Only sets the JsonbContainer pointer for binary JSON values (jbvBinary type)
- The base object context is crucial for implementing JSONPath methods that need access to parent container information
- Part of the state management system for complex JSONPath operations that require contextual information about containing objects
- Used in conjunction with operations that may need to restore the previous base object state

## Simplified Source

```c
static JsonBaseObjectInfo
setBaseObject(JsonPathExecContext *cxt, JsonbValue *jbv, int32 id)
{
    // Save current base object info for restoration
    JsonBaseObjectInfo previous_base = cxt->baseObject;

    // Set new base object - extract container from binary values
    cxt->baseObject.jbc = (jbv->type != jbvBinary) ? NULL :
                          (JsonbContainer *) jbv->val.binary.data;
    cxt->baseObject.id = id;

    return previous_base;
}
```