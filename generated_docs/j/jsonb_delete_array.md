# jsonb_delete_array

## Location
[src/backend/utils/adt/jsonfuncs.c:4693-4779](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L4693-L4779)

## Overview
Removes multiple specified keys or array elements from a JSONB value based on an array of text values, returning a new JSONB object without the deleted items.

## Definition
```c
Datum jsonb_delete_array(PG_FUNCTION_ARGS)
```

## Detailed Description
The `jsonb_delete_array` function is an extended version of `jsonb_delete` that can remove multiple keys or elements in a single operation. It takes a JSONB value and an array of text values, then returns a new JSONB value with all matching elements removed.

The function iterates through the JSONB structure and for each key or element encountered, it checks against all values in the provided array. If a match is found using string comparison, that key-value pair or array element is excluded from the result. Like `jsonb_delete`, it cannot operate on scalar JSONB values.

## Parameters / Member Variables
- `in`: Input JSONB value from which to delete the specified items
- `keys`: Array of text values specifying the keys (for objects) or element values (for arrays) to delete

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_JSONB_P
  - PG_GETARG_ARRAYTYPE_P
  - ARR_NDIM
  - JB_ROOT_IS_SCALAR
  - JB_ROOT_COUNT
  - [deconstruct_array_builtin](../d/deconstruct_array_builtin.md)
  - [JsonbIteratorInit](../J/JsonbIteratorInit.md)
  - [JsonbIteratorNext](../J/JsonbIteratorNext.md)
  - [pushJsonbValue](../p/pushJsonbValue.md)
  - [JsonbValueToJsonb](../J/JsonbValueToJsonb.md)
  - PG_RETURN_JSONB_P
  - ereport/ERROR
- Called from (representative examples):
  - No direct callers found (exposed as SQL function)

## Notes and Other Information
- Throws an error if applied to a scalar JSONB value
- Validates that the keys array is single-dimensional
- Returns the original JSONB unchanged if it is empty or the keys array is empty
- Performs string comparison using memcmp for each key/element against all array values
- When deleting object keys, also skips the corresponding values
- Uses skipNested flag for efficient nested structure traversal
- Skips null elements in the keys array
- Does not modify the original JSONB; returns a new copy
- More efficient than multiple individual delete operations
- Exposed as the SQL function `jsonb_delete(jsonb, text[])`