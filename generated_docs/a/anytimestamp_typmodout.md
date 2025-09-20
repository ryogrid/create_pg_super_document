# anytimestamp_typmodout

## Location
[src/backend/utils/adt/timestamp.c:145-163](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L145-L163)

## Overview
A static helper function that formats type modifier information for TIMESTAMP and TIMESTAMP WITH TIME ZONE data types into a human-readable string representation.

## Definition

```c
struct pg_tm tt,
			   *tm = &tt;
```
## Detailed Description
This function serves as common code for both timestamptypmodout and timestamptztypmodout functions. It generates a string representation of the timestamp type with its precision modifier, following SQL standard syntax. The function formats the output to show the precision value (if specified) and the appropriate timezone qualifier.

When a valid precision is specified (typmod >= 0), the output includes the precision in parentheses followed by the timezone qualifier. When no precision is specified (typmod < 0), only the timezone qualifier is included.

## Parameters / Member Variables
- `istz`: Boolean flag indicating whether this is for a timezone-aware timestamp type (determines "with time zone" vs "without time zone" in output)
- `typmod`: The precision value to format; negative values indicate no precision specified

## Dependencies
- Functions called/Symbols referenced:
  - [psprintf](../p/psprintf.md) (for formatted string creation)
  - [pstrdup](../p/pstrdup.md) (for string duplication)
- Called from:
  - [timestamptypmodout](../t/timestamptypmodout.md) (src/backend/utils/adt/timestamp.c:314)
  - [timestamptztypmodout](../t/timestamptztypmodout.md) (src/backend/utils/adt/timestamp.c:870)

## Notes and Other Information
- Returns a newly allocated string that must be freed by the caller
- Output format examples: "(3) without time zone", "(6) with time zone", " without time zone"
- The function handles both TIMESTAMP and TIMESTAMPTZ types through the istz parameter
- Used in PostgreSQL's type system for displaying type information in error messages, DESCRIBE commands, and system catalogs
- Always includes a space before the timezone qualifier for consistent formatting