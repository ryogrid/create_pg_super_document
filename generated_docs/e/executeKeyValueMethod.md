# executeKeyValueMethod

## Location
[src/backend/utils/adt/jsonpath_exec.c:2820-2929](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L2820-L2929)

## Overview
Implements the JSON path .keyvalue() method that transforms object properties into a sequence of structured key-value pair objects.

## Definition
```c
static JsonPathExecResult executeKeyValueMethod(JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jb, JsonValueList *found)
```

## Detailed Description
The `executeKeyValueMethod` function implements the .keyvalue() method for JSON path expressions, which converts each key-value pair in a JSON object into a structured object with "key", "value", and "id" fields. The "id" field provides a unique identifier constructed from the base object ID and the binary offset within that object, using the formula: id = 10000000000 * base_object_id + obj_offset_in_base_object. This method facilitates object introspection and enables tracking of object relationships in complex JSON path operations.

## Parameters / Member Variables
- `cxt`: JsonPathExecContext pointer providing execution context and base object tracking
- `jsp`: JsonPathItem pointer representing the .keyvalue() method being executed  
- `jb`: JsonbValue pointer to the input JSON object to process
- `found`: JsonValueList pointer for collecting resulting key-value pair objects

## Dependencies
- Functions called/Symbols referenced:
  - [JsonbType](../J/JsonbType.md): Get type of JsonbValue
  - JsonContainerSize: Get number of elements in JSON container
  - [jspGetNext](../j/jspGetNext.md): Get next item in JSON path
  - [JsonbIteratorInit](../J/JsonbIteratorInit.md)/JsonbIteratorNext: Iterate through JSON object pairs
  - [pushJsonbValue](../p/pushJsonbValue.md): Build JSON objects programmatically
  - [JsonbValueToJsonb](../J/JsonbValueToJsonb.md): Convert JsonbValue to Jsonb format
  - [JsonbInitBinary](../J/JsonbInitBinary.md): Initialize binary JsonbValue
  - [int64_to_numeric](../i/int64_to_numeric.md): Convert integer to PostgreSQL numeric type
  - [setBaseObject](../s/setBaseObject.md): Set base object context for ID generation
  - [executeNextItem](executeNextItem.md): Continue JSON path evaluation
  - [jspOperationName](../j/jspOperationName.md): Get operation name for error messages
- Called from (representative examples):
  - [executeItemOptUnwrapTarget](executeItemOptUnwrapTarget.md): Main item execution dispatcher
  - RETURN_ERROR: Error handling macro

## Notes and Other Information
- Returns JsonPathExecResult (jperOk on success, jperNotFound if no pairs found, jperError on failure)
- Only accepts JSON objects as input; other types result in SQL_JSON_OBJECT_NOT_FOUND error
- Generates unique object identifiers using decimal multiplier (10^10) for readability
- Base object ID 0 represents root context '$', positive numbers represent variables ''
- Creates structured objects with format: {"key": key, "value": value, "id": id}
- Updates base object context and increments lastGeneratedObjectId for each generated object
- Supports empty objects by returning jperNotFound when no key-value pairs exist
- Part of PostgreSQL's JSON path expression evaluation system for object introspection
- Uses JsonbIterator for efficient traversal of binary JSON data structures