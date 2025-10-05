# timetypmodin

## Location
[src/backend/utils/adt/date.c:1558-1565](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L1558-L1565)

## Overview
Parses and validates type modifier input for the TIME data type, converting precision specifications into internal typmod format.

## Definition

```c
Datum
timetypmodin(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL type modifier input function for the TIME data type. It processes an array of type modifier values (typically precision specifications like in TIME(3)) and converts them into the internal integer typmod representation. The function delegates the actual parsing to  with the first parameter set to  to indicate this is for TIME (not TIMETZ).

Type modifiers for TIME typically specify the precision of fractional seconds, ranging from 0 to 6 decimal places.

## Parameters / Member Variables
-  (ArrayType*): Array containing the type modifier specifications from SQL syntax (e.g., the '3' from TIME(3))

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts ArrayType argument from function call
  - : Common function for processing time-related type modifiers
  - : Returns the processed typmod as a 32-bit integer
- Types used:
  - : PostgreSQL array type for holding modifier values

## Notes and Other Information
- This function handles type modifier parsing for the TIME data type (without timezone)
- The  parameter passed to  distinguishes TIME from TIMETZ processing
- Type modifiers for TIME are primarily precision specifiers (0-6 fractional digits)
- Part of PostgreSQL's type system infrastructure for handling parameterized types
- Located in src/backend/utils/adt/date.c:1558-1565
- Companion function to  for type modifier output

## Simplified Source

```c
Datum
timetypmodin(PG_FUNCTION_ARGS)
{
    // Extract type modifier array (e.g., precision from TIME(3))
    ArrayType *ta = PG_GETARG_ARRAYTYPE_P(0);

    // Delegate to common time typmod processing (false = TIME, not TIMETZ)
    PG_RETURN_INT32(anytime_typmodin(false, ta));
}
```