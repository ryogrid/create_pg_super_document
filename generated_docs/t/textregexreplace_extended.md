# textregexreplace_extended

## Location
[src/backend/utils/adt/regexp.c:699-743](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L699-L743)

## Overview
Provides extended regular expression replacement functionality with additional parameters for start position and occurrence selection, supporting both single and multiple replacements.

## Definition

```c
Datum
textregexreplace_extended(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function implements the full-featured version of PostgreSQL's  SQL function, supporting up to 6 arguments. It extends the basic  by allowing users to specify a start position within the source text and control which occurrence(s) to replace.

The function accepts optional parameters through PostgreSQL's variable argument mechanism (). When the occurrence count (n) is not provided, it automatically deduces the behavior from the 'g' (global) flag: 0 for global replacement or 1 for single replacement.

Input validation ensures that the start position is positive and the occurrence count is non-negative. The start position is converted to zero-based indexing before passing to the underlying replacement function.

## Parameters / Member Variables
-  (text*): Source text to search within
-  (text*): Regular expression pattern to match
-  (text*): Replacement text to substitute matches
-  (int): Starting position in the source text (1-based, default: 1)
-  (int): Occurrence number to replace (0 = all occurrences, default: 1 or deduced from flags)
-  (text*): Optional regex flags string controlling matching behavior

## Dependencies
- Functions called/Symbols referenced:
  -  (macro to get optional text argument)
  -  (struct type for regex flags)
  -  (macro to get number of arguments)
  -  (parses option string into flags structure)
  -  (performs the actual regex replacement)
  -  (macro to return text result)
  -  (macro to get current collation)
- Called from (representative examples):
  -  (src/backend/utils/adt/regexp.c:746)
  -  (src/backend/utils/adt/regexp.c:753)

## Notes and Other Information
- This is the most comprehensive version of the regexp_replace function family
- Serves as the implementation backend for other regexp_replace variants
- Supports variable-length argument lists through PostgreSQL's function argument system
- Automatically handles the global replacement logic when 'n' parameter is omitted
- Uses 1-based indexing for the start parameter (converted internally to 0-based)
- Provides comprehensive error checking for parameter validity
- Part of PostgreSQL's regular expression functionality in the regexp.c module

## Simplified Source

```c
Datum
textregexreplace_extended(PG_FUNCTION_ARGS)
{
    // Extract input parameters
    text *source_text = PG_GETARG_TEXT_PP(0);
    text *pattern = PG_GETARG_TEXT_PP(1);
    text *replacement = PG_GETARG_TEXT_PP(2);
    int start = 1;  // Default start position
    int occurrence_count = 1;  // Default occurrence to replace
    text *flags = PG_GETARG_TEXT_PP_IF_EXISTS(5);
    pg_re_flags regex_flags;

    // Handle optional start parameter
    if (PG_NARGS() > 3) {
        start = PG_GETARG_INT32(3);
        if (start <= 0) {
            ereport(ERROR, "start position must be positive");
        }
    }

    // Handle optional occurrence count parameter
    if (PG_NARGS() > 4) {
        occurrence_count = PG_GETARG_INT32(4);
        if (occurrence_count < 0) {
            ereport(ERROR, "occurrence count must be non-negative");
        }
    }

    // Parse regex flags from flags string
    parse_re_flags(&regex_flags, flags);

    // If occurrence count not specified, determine from 'g' flag
    if (PG_NARGS() <= 4) {
        occurrence_count = regex_flags.glob ? 0 : 1;  // 0 = all, 1 = first
    }

    // Perform the replacement operation
    // Convert 1-based start position to 0-based for internal use
    PG_RETURN_TEXT_P(replace_text_regexp(source_text, pattern, replacement,
                                       regex_flags.cflags, PG_GET_COLLATION(),
                                       start - 1, occurrence_count));
}
```