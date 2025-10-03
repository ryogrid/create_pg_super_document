# timestamptztypmodout

## Location
[src/backend/utils/adt/timestamp.c:866-878](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L866-L878)

## Overview
Converts internal typmod representation back to external string format for the timestamptz data type.

## Definition

```c
Datum
timestamptztypmodout(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function serves as the type modifier output function for the timestamptz data type. It takes PostgreSQL's internal typmod representation (an integer) and converts it back to a human-readable string format that can be displayed to users or used in SQL output. This function is the counterpart to  and is used when PostgreSQL needs to display type information, such as in DESCRIBE statements, pg_dump output, or error messages.

The function delegates the actual formatting logic to , passing  to indicate this is for a timestamptz type (with timezone) rather than a plain timestamp type.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: int32 containing the internal typmod representation (from )
## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract the typmod argument
  - : Shared function that handles typmod formatting for timestamp types
  - : Macro to return the formatted string result
- Called from (representative examples):
  - No direct references found (used internally by PostgreSQL's type system)

## Notes and Other Information
- Located in src/backend/utils/adt/timestamp.c:866-878
- Part of PostgreSQL's type system infrastructure for displaying type information
- The function is a thin wrapper around  with the timezone flag set to true
- Used when PostgreSQL needs to display timestamptz type information with modifiers
- Typically converts precision specifiers back to formats like "(3)" for 3-digit fractional seconds
- Called automatically by PostgreSQL's type system infrastructure during introspection operations
- Counterpart to  for bidirectional typmod conversion
- Essential for tools like pg_dump to accurately recreate table definitions