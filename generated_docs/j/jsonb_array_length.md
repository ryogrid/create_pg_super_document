# jsonb_array_length

## Location
[src/backend/utils/adt/jsonfuncs.c:1876-1897](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L1876-L1897)

## Overview
A SQL function that returns the number of elements in a JSONB array as an integer, providing efficient array length calculation for the binary JSON format.

## Definition
```c
Datum jsonb_array_length(PG_FUNCTION_ARGS)
```

## Detailed Description
The `jsonb_array_length` function implements the SQL function `jsonb_array_length(jsonb) -> int` which efficiently counts the number of elements in a JSONB array. Unlike its JSON counterpart that requires parsing, this function operates directly on the binary JSONB format for optimal performance. It validates that the input is an array (not a scalar or object) and uses the JSONB root container's built-in count information to return the array length immediately without iteration.

## Parameters / Member Variables
- Takes a single argument via PG_FUNCTION_ARGS:
  - `jb`: A Jsonb value containing the JSONB array to measure

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_JSONB_P
  - JB_ROOT_IS_SCALAR
  - JB_ROOT_IS_ARRAY
  - JB_ROOT_COUNT
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - PG_RETURN_INT32
- Types used:
  - Jsonb
  - JsonParseErrorType
- Called from:
  - No direct callers found (SQL-callable function)

## Notes and Other Information
- This is a SQL-callable function exposed to PostgreSQL users
- More efficient than json_array_length as it operates on binary JSONB format
- Performs input validation with specific error messages for scalar and non-array inputs
- Uses JSONB macros (JB_ROOT_IS_SCALAR, JB_ROOT_IS_ARRAY, JB_ROOT_COUNT) for efficient operations
- Returns ERRCODE_INVALID_PARAMETER_VALUE for invalid input types
- The count is readily available from the JSONB root container structure, making this O(1) operation
- Companion function to json_array_length but for the binary JSONB format
- Part of PostgreSQL's JSONB functionality for high-performance JSON operations

## Simplified Source
```c
Datum jsonb_array_length(PG_FUNCTION_ARGS) {
    Jsonb *jb = PG_GETARG_JSONB_P(0);

    // Validate input: must be an array, not scalar or object
    if (JB_ROOT_IS_SCALAR(jb))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("cannot get array length of a scalar")));

    if (!JB_ROOT_IS_ARRAY(jb))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("cannot get array length of a non-array")));

    // Return count directly from JSONB root container (O(1) operation)
    PG_RETURN_INT32(JB_ROOT_COUNT(jb));
}
```