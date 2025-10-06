# textregexeq

## Location
[src/backend/utils/adt/regexp.c:487-500](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L487-L500)

## Overview
The textregexeq function performs regular expression matching on PostgreSQL text data types, returning true if the text matches the provided regular expression pattern.

## Definition
```c
Datum textregexeq(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL interface routine called by the function manager to implement the ~= operator for text data types. It takes two text arguments: the source text string to be tested and a text pattern containing a regular expression, then determines if the text matches the pattern. The function uses the RE_compile_and_execute utility with advanced regular expression features enabled and respects the current collation settings for locale-aware matching. Unlike nameregexeq which works with fixed-length name types, this function handles variable-length text data.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: Text value to be tested against the pattern
  - Argument 1: Text containing the regular expression pattern

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP: Extracts text arguments from function arguments (for both source and pattern)
  - [RE_compile_and_execute](../R/RE_compile_and_execute.md): Compiles and executes the regular expression
  - VARDATA_ANY: Gets pointer to the actual text data within the varlena structure
  - VARSIZE_ANY_EXHDR: Gets the size of text data excluding the varlena header
  - PG_GET_COLLATION: Gets current collation for locale-aware matching
  - PG_RETURN_BOOL: Returns boolean result to PostgreSQL
  - REG_ADVANCED: Flag for advanced regular expression features
- Called from (representative examples):
  - No direct callers found (typically invoked through PostgreSQL operator system)

## Notes and Other Information
- This function implements the ~= operator for text types in PostgreSQL SQL queries
- Uses REG_ADVANCED flag to enable advanced regular expression features
- Respects collation settings for proper locale-aware string matching
- Handles variable-length text data using VARDATA_ANY and VARSIZE_ANY_EXHDR macros
- The function is part of PostgreSQL's regular expression infrastructure in src/backend/utils/adt/regexp.c
- Returns a Datum containing a boolean value indicating match success
- Differs from nameregexeq in that it works with variable-length text rather than fixed-length names

## Simplified Source

```c
Datum textregexeq(PG_FUNCTION_ARGS) {
    text *source = PG_GETARG_TEXT_PP(0);
    text *pattern = PG_GETARG_TEXT_PP(1);

    // Return result of regex matching on text
    PG_RETURN_BOOL(RE_compile_and_execute(pattern,
                                         VARDATA_ANY(source),
                                         VARSIZE_ANY_EXHDR(source),
                                         REG_ADVANCED,
                                         PG_GET_COLLATION(),
                                         0, NULL));
}
```