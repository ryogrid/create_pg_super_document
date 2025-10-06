# similar_escape_internal

## Location
[src/backend/utils/adt/regexp.c:767-1031](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L767-L1031)

## Overview
Converts SQL "SIMILAR TO" regexp patterns to POSIX style for use with PostgreSQL's regexp engine, handling escape sequences and special character transformations.

## Definition

```c
static text *
similar_escape_internal(text *pat_text, text *esc_text)
```
## Detailed Description
The  function serves as the core implementation for PostgreSQL's "SIMILAR TO" pattern matching functionality. It transforms SQL standard "SIMILAR TO" patterns into POSIX-compatible regular expressions that can be processed by PostgreSQL's regexp engine.

The function performs several key transformations:
- Wraps the pattern with  to ensure full string matching as required by SQL spec
- Handles escape-double-quote sequences for SUBSTRING pattern separation
- Converts SQL wildcards ( to ,  to )
- Processes character classes with proper bracket handling
- Manages escape sequences and special character escaping
- Supports multi-byte character encodings

For SUBSTRING operations, the function creates a three-part pattern structure with specific greedy/non-greedy quantifiers to ensure SQL-compliant behavior where the middle part (between escape-double-quotes) captures the largest possible match.

## Parameters / Member Variables
-  (text*): The input SQL "SIMILAR TO" pattern to be converted
-  (text*): The escape character specification (NULL for default '\', empty string for no escape)

## Dependencies
- Functions called/Symbols referenced:
  -  (calculates multi-byte string length)
  -  (macro to access variable-length data)
  -  (gets multi-byte character length)
  -  (macro to set variable-length data size)
- Called from (representative examples):
  -  (src/backend/utils/adt/regexp.c:1038)
  -  (src/backend/utils/adt/regexp.c:1053)
  -  (src/backend/utils/adt/regexp.c:1082)

## Notes and Other Information
- Static function serving as the common implementation for three SQL-exposed functions
- Handles complex pattern transformation including character class nesting and escape-double-quote separators
- Supports both single-byte and multi-byte character encodings with optimized fast/slow paths
- Implements SQL standard requirements for pattern anchoring and greedy matching behavior
- Enforces SQL spec limitation of at most two escape-double-quote separators
- Allocates result buffer with sufficient space (up to 3 bytes output per input byte)
- Critical component for PostgreSQL's SIMILAR TO and SUBSTRING pattern matching functionality

## Simplified Source

```c
static text *
similar_escape_internal(text *pattern_text, text *escape_text)
{
    // Initialize variables for pattern processing
    char *pattern_data = VARDATA_ANY(pattern_text);
    int pattern_length = VARSIZE_ANY_EXHDR(pattern_text);
    char *escape_char = NULL;
    int escape_length = 0;
    bool after_escape = false;
    int quote_count = 0;
    int bracket_depth = 0;  // Nesting level of character classes [...]
    int bracket_start = 0;  // State for handling bracket start
    text *result;
    char *result_ptr;

    // Determine escape character
    if (escape_text == NULL) {
        escape_char = "\\";  // Default backslash escape
        escape_length = 1;
    } else {
        escape_char = VARDATA_ANY(escape_text);
        escape_length = VARSIZE_ANY_EXHDR(escape_text);
        if (escape_length == 0) {
            escape_char = NULL;  // No escape character
        } else if (escape_length > 1) {
            // Validate single character escape
            if (pg_mbstrlen_with_len(escape_char, escape_length) > 1) {
                ereport(ERROR, "Escape string must be empty or one character");
            }
        }
    }

    // Allocate result buffer (up to 3 bytes output per input byte + overhead)
    result = (text *) palloc(VARHDRSZ + 23 + 3 * (size_t) pattern_length);
    result_ptr = VARDATA(result);

    // Begin POSIX pattern: ^(?:
    *result_ptr++ = '^';
    *result_ptr++ = '(';
    *result_ptr++ = '?';
    *result_ptr++ = ':';

    // Process each character in the pattern
    while (pattern_length > 0) {
        char current_char = *pattern_data;

        // Handle multi-byte characters if escape is multi-byte
        if (escape_length > 1) {
            int mblen = pg_mblen(pattern_data);
            if (mblen > 1) {
                // Multi-byte character processing
                if (after_escape) {
                    *result_ptr++ = '\\';
                    memcpy(result_ptr, pattern_data, mblen);
                    result_ptr += mblen;
                    after_escape = false;
                } else if (escape_char && escape_length == mblen &&
                          memcmp(escape_char, pattern_data, mblen) == 0) {
                    after_escape = true;  // Found escape sequence
                } else {
                    memcpy(result_ptr, pattern_data, mblen);
                    result_ptr += mblen;
                }
                pattern_data += mblen;
                pattern_length -= mblen;
                continue;
            }
        }

        // Single-byte character processing
        if (after_escape) {
            if (current_char == '"' && bracket_depth < 1) {
                // Handle escape-double-quote for SUBSTRING pattern parts
                if (quote_count == 0) {
                    // End part1, begin part2: ){1,1}?(
                    strcpy(result_ptr, "){1,1}?(");
                    result_ptr += 8;
                } else if (quote_count == 1) {
                    // End part2, begin part3: ){1,1}(?:
                    strcpy(result_ptr, "){1,1}(?:");
                    result_ptr += 9;
                } else {
                    ereport(ERROR, "Too many escape-double-quote separators");
                }
                quote_count++;
            } else {
                // Escape any character
                *result_ptr++ = '\\';
                *result_ptr++ = current_char;
            }
            after_escape = false;
        } else if (escape_char && current_char == *escape_char) {
            after_escape = true;  // Found escape character
        } else if (bracket_depth > 0) {
            // Inside character class - handle brackets and special cases
            if (current_char == '\\') *result_ptr++ = '\\';
            *result_ptr++ = current_char;

            // Track bracket nesting
            if (current_char == ']' && bracket_start > 2) bracket_depth--;
            else if (current_char == '[') bracket_depth++;

            // Handle caret positioning in character classes
            if (current_char == '^') bracket_start++;
            else bracket_start = 3;
        } else {
            // Outside character class - handle SQL wildcards and special chars
            switch (current_char) {
                case '[':
                    *result_ptr++ = current_char;
                    bracket_depth++;
                    bracket_start = 1;
                    break;
                case '%':
                    *result_ptr++ = '.';
                    *result_ptr++ = '*';
                    break;
                case '_':
                    *result_ptr++ = '.';
                    break;
                case '(':
                    // Convert to non-capturing group
                    *result_ptr++ = '(';
                    *result_ptr++ = '?';
                    *result_ptr++ = ':';
                    break;
                case '\\':
                case '.':
                case '^':
                case '$':
                    // Escape regex metacharacters
                    *result_ptr++ = '\\';
                    *result_ptr++ = current_char;
                    break;
                default:
                    *result_ptr++ = current_char;
                    break;
            }
        }
        pattern_data++;
        pattern_length--;
    }

    // End POSIX pattern: )$
    *result_ptr++ = ')';
    *result_ptr++ = '$';

    // Set final result size
    SET_VARSIZE(result, result_ptr - ((char *) result));
    return result;
}
```