# jsonb_strip_nulls

## Location
src/backend/utils/adt/jsonfuncs.c: 4525 - 4582

## Overview
Removes all key-value pairs with null values from a JSONB object, returning a new JSONB object without the null-valued fields.

## Definition
```c
Datum jsonb_strip_nulls(PG_FUNCTION_ARGS)
```

## Detailed Description
The `jsonb_strip_nulls` function is a SQL function that takes a JSONB value and returns a copy with all key-value pairs containing null values removed. It works by iterating through the JSONB structure using JsonbIterator and selectively copying only non-null key-value pairs to a new JSONB object.

The function handles scalar JSONB values by returning them unchanged since scalars cannot contain key-value pairs. For objects, it uses a state machine approach where it temporarily stores keys and only adds them to the result if the corresponding value is not null.

## Parameters / Member Variables
- `jb`: Input JSONB value from which null-valued key-value pairs will be stripped

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_JSONB_P
  - JB_ROOT_IS_SCALAR
  - JsonbIteratorInit
  - JsonbIteratorNext
  - pushJsonbValue
  - JsonbValueToJsonb
- Called from (representative examples):
  - No direct callers found (exposed as SQL function)

## Notes and Other Information
- Only removes key-value pairs where the value is explicitly null (jbvNull)
- Scalar JSONB values are returned unchanged
- Uses delayed key processing to avoid adding keys for null values
- The function preserves the structure of nested objects and arrays while only removing null-valued pairs at all levels
- Exposed as the SQL function `jsonb_strip_nulls(jsonb)`