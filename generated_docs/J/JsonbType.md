# JsonbType

## Location
src/backend/utils/adt/jsonpath_exec.c: 3614 - 3637

## Overview
JsonbType is a static function that determines the actual type of a JsonbValue, resolving binary containers to their underlying object or array types.

## Definition
static int JsonbType(JsonbValue *jb)

## Detailed Description
This function returns the effective type of a JsonbValue structure. For most JsonbValue types, it simply returns the type field. However, for jbvBinary values (which represent binary JSONB containers), it inspects the actual container to determine if it's an object (jbvObject) or array (jbvArray). This is necessary because binary JSONB data can contain either objects or arrays, and the JSON path execution engine needs to know the specific type for proper processing. The function includes assertions to ensure that scalar values are not encountered in binary form during JSON path execution, as they should have been extracted earlier in the process.

## Parameters / Member Variables
- `jb`: Pointer to the JsonbValue whose type needs to be determined

## Dependencies
- Functions called/Symbols referenced:
  - jbvBinary (JsonbValue type constant for binary data)
  - JsonbContainer (structure representing JSONB binary containers)
  - JsonContainerIsScalar (macro to check if container holds a scalar value)
  - JsonContainerIsObject (macro to check if container holds an object)
  - jbvObject (JsonbValue type constant for objects)
  - JsonContainerIsArray (macro to check if container holds an array)
  - jbvArray (JsonbValue type constant for arrays)
  - elog (PostgreSQL error logging function)
- Called from (representative examples):
  - executeItemOptUnwrapTarget (extensively used)
  - executeItemOptUnwrapResult
  - executeNumericItemMethod
  - executeKeyValueMethod

## Notes and Other Information
- This is a static function internal to jsonpath_exec.c, not exposed in the public API
- Never returns jbvBinary as the result type; always resolves to the underlying container type
- Includes an assertion that scalar values should not appear in binary form during JSON path execution
- Throws an ERROR if an invalid JSONB container type is encountered
- Essential for type checking and dispatch logic in JSON path operations
- The function handles the abstraction layer between PostgreSQL's binary JSONB storage format and the JSON path execution engine's type system