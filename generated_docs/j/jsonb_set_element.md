# jsonb_set_element

## Location
[src/backend/utils/adt/jsonfuncs.c:1677-1699](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L1677-L1699)

## Overview
A function that sets or updates an element in a JSONB structure at a specified path, creating intermediate containers as needed.

## Definition

```c
Datum
jsonb_set_element(Jsonb *jb, Datum *path, int path_len,
				  JsonbValue *newval)
```
## Detailed Description
The  function provides the capability to modify JSONB data structures by setting values at specific paths. It uses the  function internally to perform the actual path-based modification, with flags that enable creating missing intermediate objects/arrays and filling gaps in arrays. The function handles the special case of raw scalar arrays by extracting the first element. It initializes a JSONB iterator to traverse the existing structure and applies the modification using a parse state to build the result.

## Parameters / Member Variables
- `*jb`: Input JSONB structure to modify
- `*path`: Array of Datum values representing the path where to set the value
- `path_len`: Number of elements in the path array
- `*newval`: JsonbValue containing the new value to set at the specified path
## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md) (memory allocation)
  - [JsonbIteratorInit](../J/JsonbIteratorInit.md) (iterator initialization)
  - [setPath](../s/setPath.md) (core path modification logic)
  - [pfree](../p/pfree.md) (memory deallocation)
  - [JsonbValueToJsonb](../J/JsonbValueToJsonb.md) (convert JsonbValue to Jsonb)
  - PG_RETURN_JSONB_P (return JSONB result)
  - Constants: JB_PATH_CREATE, JB_PATH_FILL_GAPS, JB_PATH_CONSISTENT_POSITION
- Called from (representative examples):
  - [jsonb_subscript_assign](jsonb_subscript_assign.md) (subscript assignment operations)

## Notes and Other Information
- Located in src/backend/utils/adt/jsonfuncs.c:1677-1699
- Uses setPath with creation flags to enable setting values in previously non-existent paths
- JB_PATH_CREATE: Creates missing intermediate containers (objects/arrays)
- JB_PATH_FILL_GAPS: Fills gaps in arrays when setting beyond current bounds
- JB_PATH_CONSISTENT_POSITION: Maintains consistent positioning in the result
- Handles raw scalar arrays by unwrapping the first element
- Allocates temporary path_nulls array to track null path elements
- Returns a new JSONB structure with the modification applied (immutable operation)

## Simplified Source

```c
Datum jsonb_set_element(Jsonb *jb, Datum *path, int path_len, JsonbValue *newval) {
    JsonbValue *res;
    JsonbParseState *state = NULL;
    JsonbIterator *it;
    bool *path_nulls = palloc0(path_len * sizeof(bool));

    // Handle raw scalar arrays by extracting the first element
    if (newval->type == jbvArray && newval->val.array.rawScalar)
        *newval = newval->val.array.elems[0];

    // Initialize iterator for the input JSONB
    it = JsonbIteratorInit(&jb->root);

    // Perform the actual path modification
    res = setPath(&it, path, path_nulls, path_len, &state, 0, newval,
                  JB_PATH_CREATE | JB_PATH_FILL_GAPS | JB_PATH_CONSISTENT_POSITION);

    pfree(path_nulls);

    PG_RETURN_JSONB_P(JsonbValueToJsonb(res));
}
```