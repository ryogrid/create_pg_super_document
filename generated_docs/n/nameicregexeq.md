# nameicregexeq

## Location
[src/backend/utils/adt/regexp.c:522-535](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L522-L535)

## Overview
The nameicregexeq function performs case-insensitive regular expression matching on PostgreSQL name data types, returning true if the name matches the provided regular expression pattern regardless of case.

## Definition
```c
Datum nameicregexeq(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL interface routine called by the function manager to implement the ~* operator for name data types. It takes a PostgreSQL name (a fixed-length string type used for identifiers) and a text pattern containing a regular expression, then determines if the name matches the pattern using case-insensitive comparison. The function uses the RE_compile_and_execute utility with both advanced regular expression features and case-insensitive matching enabled via the REG_ICASE flag. It respects the current collation settings for proper locale-aware string matching.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: Name value to be tested against the pattern
  - Argument 1: Text containing the regular expression pattern

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NAME: Extracts name argument from function arguments
  - PG_GETARG_TEXT_PP: Extracts text argument (pattern) from function arguments
  - [RE_compile_and_execute](../R/RE_compile_and_execute.md): Compiles and executes the regular expression
  - NameStr: Converts Name to null-terminated string
  - strlen: Calculates string length
  - PG_GET_COLLATION: Gets current collation for locale-aware matching
  - PG_RETURN_BOOL: Returns boolean result to PostgreSQL
  - REG_ADVANCED: Flag for advanced regular expression features
  - REG_ICASE: Flag for case-insensitive matching
- Called from (representative examples):
  - No direct callers found (typically invoked through PostgreSQL operator system)

## Notes and Other Information
- This function implements the ~* (case-insensitive match) operator for name types in PostgreSQL SQL queries
- Uses both REG_ADVANCED and REG_ICASE flags to enable advanced features and case-insensitive matching
- Respects collation settings for proper locale-aware string matching
- The function is part of PostgreSQL's regular expression infrastructure in src/backend/utils/adt/regexp.c
- Returns a Datum containing a boolean value indicating match success
- Differs from nameregexeq by adding the REG_ICASE flag for case-insensitive operation
- Part of the case-insensitive regex family of functions as noted in the source comment
- Particularly useful for matching identifiers where case sensitivity is not desired

## Simplified Source

```c
Datum nameicregexeq(PG_FUNCTION_ARGS) {
    Name name = PG_GETARG_NAME(0);
    text *pattern = PG_GETARG_TEXT_PP(1);

    // Return result of case-insensitive regex matching
    PG_RETURN_BOOL(RE_compile_and_execute(pattern,
                                         NameStr(*name),
                                         strlen(NameStr(*name)),
                                         REG_ADVANCED | REG_ICASE,
                                         PG_GET_COLLATION(),
                                         0, NULL));
}
```