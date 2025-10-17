# jsonb_set

## Location
[src/backend/utils/adt/jsonfuncs.c:4844-4892](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L4844-L4892)

## Overview
Sets a value at a specified path in a JSONB structure, with optional creation of missing path components.

## Definition

```c
struct_array_builtin(path, TEXTOID, &path_elems, &path_nulls, &path_len);
```
## Detailed Description
The  function is a SQL-callable function that sets a value at a specified path within a JSONB structure. It accepts a path array specifying the location where to set the value, a new JSONB value to set, and a boolean flag indicating whether to create missing path components. The function traverses the JSONB structure using the provided path and either replaces an existing value or creates new path elements as needed.

The function validates input parameters, ensures the root is not a scalar, and uses the internal  function to perform the actual modification. It returns a new JSONB structure with the modification applied, following PostgreSQL's immutable data approach.

## Parameters / Member Variables
- : The input JSONB structure to modify
- : Array of text elements defining the path to the target location
- : The new JSONB value to set at the specified path
- : Whether to create missing path components (true) or only replace existing ones (false)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_JSONB_P: Extract JSONB argument from function call
  - PG_GETARG_ARRAYTYPE_P: Extract array argument from function call
  - PG_GETARG_BOOL: Extract boolean argument from function call
  - [JsonbToJsonbValue](../J/JsonbToJsonbValue.md): Convert Jsonb to JsonbValue
  - ARR_NDIM: Get number of array dimensions
  - JB_ROOT_IS_SCALAR: Check if JSONB root is scalar
  - JB_ROOT_COUNT: Get count of root elements
  - [deconstruct_array_builtin](../d/deconstruct_array_builtin.md): Deconstruct PostgreSQL array
  - [JsonbIteratorInit](../J/JsonbIteratorInit.md): Initialize JSONB iterator
  - [setPath](../s/setPath.md): Internal function to set value at path
  - [JsonbValueToJsonb](../J/JsonbValueToJsonb.md): Convert JsonbValue to Jsonb
  - PG_RETURN_JSONB_P: Return JSONB value from function
- Called from (representative examples):
  - [jsonb_set_lax](jsonb_set_lax.md): Called for lax version of set operation

## Notes and Other Information
- Only accepts one-dimensional path arrays
- Cannot set paths in scalar JSONB values
- When create is false and the root is empty, returns the original JSONB unchanged
- Empty path arrays return the original JSONB unchanged
- Uses JB_PATH_CREATE or JB_PATH_REPLACE modes based on the create parameter
- File location: src/backend/utils/adt/jsonfuncs.c:4844-4892

## Simplified Source

```c
Datum jsonb_set(PG_FUNCTION_ARGS) {
    Jsonb *in = PG_GETARG_JSONB_P(0);
    ArrayType *path = PG_GETARG_ARRAYTYPE_P(1);
    Jsonb *newjsonb = PG_GETARG_JSONB_P(2);
    bool create = PG_GETARG_BOOL(3);
    JsonbValue newval;
    JsonbValue *res = NULL;
    Datum *path_elems;
    bool *path_nulls;
    int path_len;
    JsonbIterator *it;
    JsonbParseState *st = NULL;

    // Convert new JSONB to JsonbValue
    JsonbToJsonbValue(newjsonb, &newval);

    // Validate path array dimensions
    if (ARR_NDIM(path) > 1)
        ereport(ERROR, (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                       errmsg("wrong number of array subscripts")));

    // Error check: cannot set path in scalar
    if (JB_ROOT_IS_SCALAR(in))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("cannot set path in scalar")));

    // Return unchanged if empty and not creating
    if (JB_ROOT_COUNT(in) == 0 && !create)
        PG_RETURN_JSONB_P(in);

    // Extract path elements
    deconstruct_array_builtin(path, TEXTOID, &path_elems, &path_nulls, &path_len);

    if (path_len == 0)
        PG_RETURN_JSONB_P(in);

    it = JsonbIteratorInit(&in->root);

    // Set value at specified path
    res = setPath(&it, path_elems, path_nulls, path_len, &st,
                  0, &newval, create ? JB_PATH_CREATE : JB_PATH_REPLACE);

    PG_RETURN_JSONB_P(JsonbValueToJsonb(res));
}
```