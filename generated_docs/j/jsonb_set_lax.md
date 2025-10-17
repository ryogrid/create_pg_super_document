# jsonb_set_lax

## Location
[src/backend/utils/adt/jsonfuncs.c:4893-4959](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L4893-L4959)

## Overview
A "lax" version of jsonb_set that provides flexible handling of NULL values through different null value treatment strategies.

## Definition

```c
struct_array_builtin(path, TEXTOID, &path_elems, &path_nulls, &path_len);
```
## Detailed Description
The  function is a SQL-callable function that extends  with flexible NULL value handling. When the new value to be set is NULL, the function provides four different treatment strategies: raise an exception, use JSON null, delete the key, or return the target unchanged. If the new value is not NULL, it simply delegates to .

The function accepts the same first four parameters as  plus an additional text parameter specifying how to handle NULL values. This makes it particularly useful in scenarios where NULL handling needs to be configurable based on application requirements.

## Parameters / Member Variables
- : The input JSONB structure to modify (argument 0)
- : Array of text elements defining the path to the target location (argument 1)
- : The new JSONB value to set at the specified path (argument 2)
- : Whether to create missing path components (argument 3)
- : Strategy for handling NULL values: "raise_exception", "use_json_null", "delete_key", or "return_target" (argument 4)

## Dependencies
- Functions called/Symbols referenced:
  - PG_ARGISNULL: Check if function argument is NULL
  - PG_RETURN_NULL: Return NULL from function
  - PG_GETARG_TEXT_P: Extract text argument from function call
  - [text_to_cstring](../t/text_to_cstring.md): Convert PostgreSQL text to C string
  - [jsonb_set](jsonb_set.md): Delegate to regular jsonb_set function
  - [jsonb_delete_path](jsonb_delete_path.md): Delete path when "delete_key" strategy is used
  - DirectFunctionCall1: Call function directly with one argument
  - [jsonb_in](jsonb_in.md): Parse JSON string to create JSONB value
  - [CStringGetDatum](../C/CStringGetDatum.md): Convert C string to PostgreSQL Datum
  - PG_GETARG_JSONB_P: Extract JSONB argument from function call
  - PG_RETURN_JSONB_P: Return JSONB value from function
- Called from (representative examples):
  - No direct callers found (SQL-callable function)

## Notes and Other Information
- Returns NULL if any of the required arguments (0, 1, 3) are NULL
- The null_value_treatment parameter (argument 4) cannot be NULL
- Four null handling strategies:
  - "raise_exception": Throws an error when new value is NULL
  - "use_json_null": Converts SQL NULL to JSON null value
  - "delete_key": Deletes the key at the specified path
  - "return_target": Returns the original JSONB unchanged
- When new value is not NULL, delegates directly to jsonb_set
- File location: src/backend/utils/adt/jsonfuncs.c:4893-4959

## Simplified Source

```c
Datum jsonb_set_lax(PG_FUNCTION_ARGS) {
    text *handle_null;
    char *handle_val;

    // Return NULL if required arguments are NULL
    if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(3))
        PG_RETURN_NULL();

    // Null handling strategy must be provided
    if (PG_ARGISNULL(4))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("null_value_treatment must be \"delete_key\", \"return_target\", \"use_json_null\", or \"raise_exception\"")));

    // If new value is not NULL, delegate to regular jsonb_set
    if (!PG_ARGISNULL(2))
        return jsonb_set(fcinfo);

    // Handle NULL new value based on strategy
    handle_null = PG_GETARG_TEXT_P(4);
    handle_val = text_to_cstring(handle_null);

    if (strcmp(handle_val, "raise_exception") == 0) {
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                       errmsg("JSON value must not be null"),
                       errdetail("Exception was raised because null_value_treatment is \"raise_exception\".")));
    }
    else if (strcmp(handle_val, "use_json_null") == 0) {
        // Convert SQL NULL to JSON null
        Datum newval = DirectFunctionCall1(jsonb_in, CStringGetDatum("null"));
        fcinfo->args[2].value = newval;
        fcinfo->args[2].isnull = false;
        return jsonb_set(fcinfo);
    }
    else if (strcmp(handle_val, "delete_key") == 0) {
        return jsonb_delete_path(fcinfo);
    }
    else if (strcmp(handle_val, "return_target") == 0) {
        Jsonb *in = PG_GETARG_JSONB_P(0);
        PG_RETURN_JSONB_P(in);
    }
    else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("null_value_treatment must be \"delete_key\", \"return_target\", \"use_json_null\", or \"raise_exception\"")));
    }

    return (Datum) 0;  // Never reached
}
```