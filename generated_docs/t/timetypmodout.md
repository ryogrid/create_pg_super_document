# timetypmodout

## Location
[src/backend/utils/adt/date.c:1566-1576](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L1566-L1576)

## Overview
Converts internal typmod representation back to human-readable string format for the TIME data type.

## Definition

```c
Datum
timetypmodout(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL type modifier output function for the TIME data type. It takes an internal integer typmod value and converts it back to its string representation for display purposes. The function delegates the actual formatting to  with the first parameter set to  to indicate this is for TIME (not TIMETZ).

This function is typically used when PostgreSQL needs to display the complete type specification (e.g., showing "TIME(3)" instead of just "TIME" when the precision is specified).

## Parameters / Member Variables
-  (int32): Internal integer representation of the type modifier specifying precision

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts int32 argument from function call
  - : Common function for formatting time-related type modifiers
  - : Returns the formatted string as a C string Datum

## Notes and Other Information
- This function handles type modifier output for the TIME data type (without timezone)
- The  parameter passed to  distinguishes TIME from TIMETZ processing
- Produces output like "(3)" for TIME(3) to show precision specification
- Part of PostgreSQL's type system infrastructure for displaying parameterized types
- Located in src/backend/utils/adt/date.c:1566-1576
- Companion function to  for type modifier input
- Used by system catalogs and introspection functions to display complete type information

## Simplified Source

```c
Datum timetypmodout(PG_FUNCTION_ARGS) {
    int32 typmod = PG_GETARG_INT32(0);

    // Delegate to common time typmod formatting function
    PG_RETURN_CSTRING(anytime_typmodout(false, typmod));
}
```