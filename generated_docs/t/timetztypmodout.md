# timetztypmodout

## Location
src/backend/utils/adt/date.c: 2391 - 2402

## Overview
Converts a TIMETZ type modifier to its string representation for display in system catalogs, error messages, and \\dt commands.

## Definition


## Detailed Description
The `timetztypmodout` function is responsible for converting internal type modifier representations back to human-readable string format for the TIMETZ (time with time zone) data type. This function is used when PostgreSQL needs to display type information to users, such as in \\dt commands, system catalog queries, or error messages.

The function takes a numeric type modifier (typically representing precision) and converts it to a string like "(3) with time zone" for TIMETZ(3). It delegates the actual formatting to `anytime_typmodout`, which is shared between TIME and TIMETZ types, passing `true` to indicate this is for a timezone-aware type.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing the type modifier as an int32

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32: Extracts the type modifier value from function args
  - anytime_typmodout: Common formatting logic for TIME/TIMETZ type modifiers (called with istz=true)
  - PG_RETURN_CSTRING: Returns the formatted string representation
- Called from (representative examples):
  - PostgreSQL system catalog functions
  - psql \\dt command handlers
  - Error reporting functions

## Notes and Other Information
- This function is registered in the PostgreSQL type system as the typmodout function for the TIMETZ type
- The output format follows SQL standard conventions (e.g., "(precision) with time zone")
- When no precision is specified (typmod < 0), returns just "with time zone"
- Shares common formatting logic with `timetypmodout` through the `anytime_typmodout` helper function
- Part of PostgreSQL's type introspection and display system