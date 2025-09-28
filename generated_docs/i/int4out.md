# int4out

## Location
[src/backend/utils/adt/int.c:298-310](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L298-L310)

## Overview
Converts a 32-bit integer (int4) to its string representation for output.

## Definition

```c
Datum
int4out(PG_FUNCTION_ARGS)
```
## Detailed Description
The int4out function is a PostgreSQL type output function that converts a 32-bit signed integer to its string representation. This function is part of the integer type system in PostgreSQL and is used internally when the database needs to output integer values as text, such as in query results or when casting integers to text format. The function allocates memory for the result string and uses the pg_ltoa utility function to perform the actual integer-to-string conversion.

## Parameters / Member Variables
- Input: 32-bit signed integer retrieved via PG_GETARG_INT32(0)
- Output: Datum containing a C-string representation of the integer

## Dependencies
- Functions called/Symbols referenced:
  - [pg_ltoa](../p/pg_ltoa.md)
  - PG_RETURN_CSTRING
- Called from (representative examples):
  - [int4_to_char](int4_to_char.md)
  - [jsonb_subscript_check_subscripts](../j/jsonb_subscript_check_subscripts.md)

## Notes and Other Information
- Allocates 12 bytes for the result string (sign + 10 digits + null terminator)
- Uses palloc for memory allocation
- The function follows PostgreSQL's fmgr (function manager) calling convention
- Located in src/backend/utils/adt/int.c:298-310

## Simplified Source

```c
// Simplified version of int4out
Datum int4out(PG_FUNCTION_ARGS) {
    int32 input_value = PG_GETARG_INT32(0);
    char *result = (char *) palloc(12);  // Sign + 10 digits + null terminator

    // Convert integer to string
    pg_ltoa(input_value, result);

    PG_RETURN_CSTRING(result);
}
```

Key simplifications made:
- Used more descriptive variable name
- Added comment explaining memory allocation size
- This function is already very simple - core logic is just integer-to-string conversion
- Focused on the essential conversion operation