# jsonb_delete_path

## Location
[src/backend/utils/adt/jsonfuncs.c:4960-5002](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L4960-L5002)

## Overview
Deletes a value at a specified path in a JSONB structure, removing the key-value pair or array element at the given location.

## Definition

```c
struct_array_builtin(path, TEXTOID, &path_elems, &path_nulls, &path_len);
```
## Detailed Description
The  function is a SQL-callable function that removes a value at a specified path within a JSONB structure. Unlike  which works only with arrays and integer indices, this function accepts a path array of text elements and can delete values from both objects and arrays. It uses the internal  function with the  mode to perform the deletion operation.

The function validates input parameters to ensure the path array is one-dimensional and the root JSONB is not a scalar. If the specified path exists, it is removed from the structure; if it doesn't exist, the original JSONB is returned unchanged.

## Parameters / Member Variables
- : The input JSONB structure from which to delete a path
- : Array of text elements defining the path to the location to delete

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_JSONB_P: Extract JSONB argument from function call
  - PG_GETARG_ARRAYTYPE_P: Extract array argument from function call
  - ARR_NDIM: Get number of array dimensions
  - JB_ROOT_IS_SCALAR: Check if JSONB root is scalar
  - JB_ROOT_COUNT: Get count of root elements
  - [deconstruct_array_builtin](../d/deconstruct_array_builtin.md): Deconstruct PostgreSQL array
  - [JsonbIteratorInit](../J/JsonbIteratorInit.md): Initialize JSONB iterator
  - [setPath](../s/setPath.md): Internal function to modify value at path (with JB_PATH_DELETE mode)
  - [JsonbValueToJsonb](../J/JsonbValueToJsonb.md): Convert JsonbValue to Jsonb
  - PG_RETURN_JSONB_P: Return JSONB value from function
- Called from (representative examples):
  - [jsonb_set_lax](jsonb_set_lax.md): Called when null_value_treatment is "delete_key"

## Notes and Other Information
- Only accepts one-dimensional path arrays
- Cannot delete paths from scalar JSONB values
- Empty path arrays return the original JSONB unchanged
- Empty root JSONB structures return the original unchanged
- Uses  mode in the  function
- Non-existent paths are handled gracefully (no error, returns original)
- Works with both object keys and array indices in the path
- File location: src/backend/utils/adt/jsonfuncs.c:4960-5002

## Simplified Source

```c
Datum jsonb_delete_path(PG_FUNCTION_ARGS) {
    Jsonb *in = PG_GETARG_JSONB_P(0);
    ArrayType *path = PG_GETARG_ARRAYTYPE_P(1);

    // Validate input: path must be 1-dimensional, input cannot be scalar
    if (ARR_NDIM(path) > 1)
        ereport(ERROR, "wrong number of array subscripts");
    if (JB_ROOT_IS_SCALAR(in))
        ereport(ERROR, "cannot delete path in scalar");

    // Handle empty cases - return original unchanged
    if (JB_ROOT_COUNT(in) == 0)
        PG_RETURN_JSONB_P(in);

    // Extract path elements from array
    Datum *path_elems;
    bool *path_nulls;
    int path_len;
    deconstruct_array_builtin(path, TEXTOID, &path_elems, &path_nulls, &path_len);

    if (path_len == 0)
        PG_RETURN_JSONB_P(in);

    // Initialize iterator and perform deletion
    JsonbIterator *it = JsonbIteratorInit(&in->root);
    JsonbParseState *st = NULL;

    // Use setPath with JB_PATH_DELETE mode to remove the path
    JsonbValue *res = setPath(&it, path_elems, path_nulls, path_len, &st,
                              0, NULL, JB_PATH_DELETE);

    return JsonbValueToJsonb(res);
}
```