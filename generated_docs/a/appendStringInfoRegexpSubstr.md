# appendStringInfoRegexpSubstr

## Location
[src/backend/utils/adt/varlena.c:4106-4205](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L4106-L4205)

## Overview
A static helper function that processes replacement text containing regular expression back references and escape sequences, appending the processed result to a StringInfo buffer.

## Definition

```c
static void
appendStringInfoRegexpSubstr(StringInfo str, text *replace_text,
							 regmatch_t *pmatch,
							 char *start_ptr, int data_pos)
```
## Detailed Description
This function implements the core logic for regexp replacement text processing in PostgreSQL. It scans through replacement text character by character, handling escape sequences and back references:

- **\1-\9**: Substitutes captured groups from the regular expression match
- **\&**: Substitutes the entire matched string  
- **\\**: Outputs a literal backslash
- **Other escapes**: Treated as literal text (backslash + character)

The function carefully handles character vs. byte positioning, using helper functions to convert between character lengths and byte lengths for proper Unicode support. It processes the replacement text in chunks, copying literal text segments efficiently and substituting back references as needed.

## Parameters / Member Variables
- : StringInfo buffer to append the processed replacement text to
- : Input text containing escape sequences and back references
- : Array of regmatch_t structures containing match positions for captured groups
- : Pointer to the beginning of the match in the source string  
- : Logical character position of start_ptr in the source string

## Dependencies
- Functions called/Symbols referenced:
  - VARDATA_ANY (macro for accessing text data)
  - VARSIZE_ANY_EXHDR (macro for getting text size excluding header)
  - memchr (C library function for finding characters)
  - [appendBinaryStringInfo](appendBinaryStringInfo.md) (append binary data to StringInfo)
  - [appendStringInfoChar](appendStringInfoChar.md) (append single character to StringInfo)
  - [charlen_to_bytelen](../c/charlen_to_bytelen.md) (convert character length to byte length)
  - regmatch_t (POSIX regex match structure)
- Called from (representative examples):
  - [replace_text_regexp](../r/replace_text_regexp.md)

## Notes and Other Information
- This is a static function internal to varlena.c, specifically designed for regexp replacement operations
- Handles Unicode properly by distinguishing between character and byte positions
- Gracefully handles edge cases like escapes at the end of strings
- The function assumes pmatch[0] contains the full match, and pmatch[1-9] contain captured groups
- Located in src/backend/utils/adt/varlena.c:4106-4205

## Simplified Source

```c
static void appendStringInfoRegexpSubstr(StringInfo str, text *replace_text,
                                        regmatch_t *pmatch,
                                        char *start_ptr, int data_pos) {
    const char *p = VARDATA_ANY(replace_text);
    const char *p_end = p + VARSIZE_ANY_EXHDR(replace_text);

    while (p < p_end) {
        const char *chunk_start = p;
        int so, eo;

        // Find next escape character
        p = memchr(p, '\\', p_end - p);
        if (p == NULL)
            p = p_end;

        // Copy literal text before escape
        if (p > chunk_start)
            appendBinaryStringInfo(str, chunk_start, p - chunk_start);

        // Done if at end, else process escape
        if (p >= p_end)
            break;
        p++;

        // Handle escape at end of string
        if (p >= p_end) {
            appendStringInfoChar(str, '\\');
            break;
        }

        // Process escape sequences
        if (*p >= '1' && *p <= '9') {
            // Back reference to captured group
            int idx = *p - '0';
            so = pmatch[idx].rm_so;
            eo = pmatch[idx].rm_eo;
            p++;
        } else if (*p == '&') {
            // Entire matched string
            so = pmatch[0].rm_so;
            eo = pmatch[0].rm_eo;
            p++;
        } else if (*p == '\\') {
            // Literal backslash
            appendStringInfoChar(str, '\\');
            p++;
            continue;
        } else {
            // Unknown escape - treat as literal
            appendStringInfoChar(str, '\\');
            continue;
        }

        // Copy the referenced text if valid match
        if (so >= 0 && eo >= 0) {
            char *chunk_start;
            int chunk_len;

            Assert(so >= data_pos);
            chunk_start = start_ptr;
            chunk_start += charlen_to_bytelen(chunk_start, so - data_pos);
            chunk_len = charlen_to_bytelen(chunk_start, eo - so);
            appendBinaryStringInfo(str, chunk_start, chunk_len);
        }
    }
}
```