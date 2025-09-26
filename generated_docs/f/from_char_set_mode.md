# from_char_set_mode

## Location
[src/backend/utils/adt/formatting.c:2400-2426](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L2400-L2426)

## Overview
A static function that sets the date mode for from-char conversion operations, with validation to prevent conflicting date mode combinations.

## Definition
```c
static bool from_char_set_mode(TmFromChar *tmfc, const FromCharDateMode mode, Node *escontext)
```

## Detailed Description
This function manages the date mode setting during string-to-date/time conversion operations in PostgreSQL's formatting system. It validates that date modes are set consistently and prevents mixing incompatible date conventions (such as Gregorian and ISO week date conventions) within the same formatting template. The function uses PostgreSQL's soft error handling mechanism through the escontext parameter, allowing callers to handle errors gracefully rather than having them thrown immediately.

## Parameters / Member Variables
- `tmfc`: Pointer to TmFromChar structure that maintains the parsing state and mode information
- `mode`: The FromCharDateMode value to set (e.g., FROM_CHAR_DATE_NONE, Gregorian, ISO week date)
- `escontext`: Node pointer for error context handling, enables soft error reporting when not NULL

## Dependencies
- Functions called/Symbols referenced:
  - FromCharDateMode (enum type)
  - TmFromChar (struct type)
  - FROM_CHAR_DATE_NONE (constant)
  - ereturn (PostgreSQL error handling macro)
- Called from (representative examples):
  - DCH_ZONED (formatting.c:1056)
  - DCH_from_char (formatting.c:3527)

## Notes and Other Information
- Returns true on success, false on failure when using soft error handling
- Prevents mixing of incompatible date conventions with descriptive error messages
- Part of PostgreSQL's robust date/time parsing infrastructure
- Uses PostgreSQL's modern error handling pattern with ErrorSaveContext support
- The function allows setting mode to FROM_CHAR_DATE_NONE without validation, as this represents no specific mode