# jsonb_insert

## Location
[src/backend/utils/adt/jsonfuncs.c:5003-5051](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L5003-L5051)

## Overview
Inserts a new value at a specified path in a JSONB structure, with the option to insert before or after an existing element.

## Definition

```c
struct_array_builtin(path, TEXTOID, &path_elems, &path_nulls, &path_len);
```
## Detailed Description
The  function is a SQL-callable function that inserts a new value at a specified path within a JSONB structure. Unlike  which replaces existing values, this function inserts new elements, primarily useful for array manipulations. The function accepts a boolean parameter to control whether the insertion happens before or after the specified location.

The function uses the internal  function with either  or  modes depending on the  parameter. It validates input parameters to ensure the path array is one-dimensional and the root JSONB is not a scalar. The insertion creates a new JSONB structure with the new element added at the appropriate position.

## Parameters / Member Variables
- : The input JSONB structure where the value will be inserted
- : Array of text elements defining the path to the insertion location
- : The new JSONB value to insert at the specified path
- : Whether to insert after (true) or before (false) the specified location

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_JSONB_P: Extract JSONB argument from function call
  - PG_GETARG_ARRAYTYPE_P: Extract array argument from function call
  - PG_GETARG_BOOL: Extract boolean argument from function call
  - [JsonbToJsonbValue](../J/JsonbToJsonbValue.md): Convert Jsonb to JsonbValue
  - ARR_NDIM: Get number of array dimensions
  - JB_ROOT_IS_SCALAR: Check if JSONB root is scalar
  - [deconstruct_array_builtin](../d/deconstruct_array_builtin.md): Deconstruct PostgreSQL array
  - [JsonbIteratorInit](../J/JsonbIteratorInit.md): Initialize JSONB iterator
  - [setPath](../s/setPath.md): Internal function to modify value at path (with JB_PATH_INSERT_BEFORE or JB_PATH_INSERT_AFTER modes)
  - [JsonbValueToJsonb](../J/JsonbValueToJsonb.md): Convert JsonbValue to Jsonb
  - PG_RETURN_JSONB_P: Return JSONB value from function
- Called from (representative examples):
  - No direct callers found (SQL-callable function)

## Notes and Other Information
- Only accepts one-dimensional path arrays
- Cannot insert into scalar JSONB values
- Empty path arrays return the original JSONB unchanged
- Uses  or  modes based on the  parameter
- Primarily designed for array insertions but can work with objects
- The insertion behavior depends on the target structure type (array vs object)
- Creates new JSONB structure rather than modifying in place
- File location: src/backend/utils/adt/jsonfuncs.c:5003-5051

## Simplified Source

```c
Datum jsonb_insert(PG_FUNCTION_ARGS) {
    Jsonb *in = PG_GETARG_JSONB_P(0);
    ArrayType *path = PG_GETARG_ARRAYTYPE_P(1);
    Jsonb *newjsonb = PG_GETARG_JSONB_P(2);
    bool after = PG_GETARG_BOOL(3);

    // Convert new value to internal format
    JsonbValue newval;
    JsonbToJsonbValue(newjsonb, &newval);

    // Validate input: path must be 1-dimensional, input cannot be scalar
    if (ARR_NDIM(path) > 1)
        ereport(ERROR, "wrong number of array subscripts");
    if (JB_ROOT_IS_SCALAR(in))
        ereport(ERROR, "cannot set path in scalar");

    // Extract path elements from array
    Datum *path_elems;
    bool *path_nulls;
    int path_len;
    deconstruct_array_builtin(path, TEXTOID, &path_elems, &path_nulls, &path_len);

    if (path_len == 0)
        PG_RETURN_JSONB_P(in);

    // Initialize iterator and perform insertion
    JsonbIterator *it = JsonbIteratorInit(&in->root);
    JsonbParseState *st = NULL;

    // Choose insert mode based on 'after' parameter
    JsonbValue *res = setPath(&it, path_elems, path_nulls, path_len, &st, 0, &newval,
                              after ? JB_PATH_INSERT_AFTER : JB_PATH_INSERT_BEFORE);

    return JsonbValueToJsonb(res);
}
```