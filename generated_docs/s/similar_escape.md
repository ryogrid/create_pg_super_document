# similar_escape

## Location
src/backend/utils/adt/regexp.c: 1066 - 1091

## Overview
A legacy PostgreSQL SQL function for converting SIMILAR TO patterns to POSIX regular expressions, maintained for compatibility with pre-v13 views.

## Definition
```c
Datum similar_escape(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements a legacy version of the PostgreSQL `similar_escape(pattern, escape)` function that provides backward compatibility with views stored using the pre-v13 expansion of SIMILAR TO expressions. Unlike the newer `similar_to_escape_1` and `similar_to_escape_2` functions, this function is non-strict, meaning it explicitly handles NULL arguments rather than relying on PostgreSQL's automatic NULL handling.

The key difference from the newer functions is in the handling of "ESCAPE NULL" - this legacy function treats NULL escape parameters by using the default escape character, which leads to not-per-spec handling according to SQL standards. This behavior is preserved for compatibility with existing stored views that may depend on this specific NULL handling.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `pat_text` (arg 0): The SIMILAR TO pattern text to be converted (checked for NULL)
  - `esc_text` (arg 1): The escape character text (can be NULL for default behavior)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_ARGISNULL` (macro for checking NULL arguments)
  - `PG_GETARG_TEXT_PP` (macro for extracting text arguments)
  - [similar_escape_internal](similar_escape_internal.md) (performs the actual pattern conversion)
  - `PG_RETURN_TEXT_P` (macro for returning text result)
  - `PG_RETURN_NULL` (macro for returning NULL result)
- Called from:
  - Legacy SQL queries and stored views using `similar_escape(pattern, escape)` function

## Notes and Other Information
- This is a legacy function maintained for backward compatibility with pre-PostgreSQL 13 views
- Unlike the newer similar_to_escape functions, this function is **non-strict**
- Explicitly handles NULL arguments with custom logic rather than using PostgreSQL's automatic NULL propagation
- When the escape parameter is NULL, uses the default escape character (backslash '\')
- When the pattern parameter is NULL, returns NULL
- Located in `src/backend/utils/adt/regexp.c:1066-1091`
- The non-spec handling of "ESCAPE NULL" is preserved intentionally for compatibility
- New applications should prefer `similar_to_escape_1` or `similar_to_escape_2` instead