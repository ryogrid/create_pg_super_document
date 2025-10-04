# str_udeescape

## Location
[src/backend/parser/parser.c:372-527](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parser.c#L372-L527)

## Overview
Processes Unicode escapes in SQL string literals, converting escape sequences like \XXXX and \+XXXXXX to their corresponding Unicode characters in the server encoding.

## Definition

```c
static char *
str_udeescape(const char *str, char escape,
			  int position, core_yyscan_t yyscanner)
```
## Detailed Description
This function is a core component of PostgreSQL's lexical analyzer that handles Unicode escape sequences in SQL string literals marked with U&'' or U&"" syntax. It parses two types of Unicode escape sequences:

1. **4-digit format**:  where XXXX is a 4-digit hexadecimal Unicode code point
2. **6-digit format**:  where XXXXXX is a 6-digit hexadecimal Unicode code point

The function properly handles UTF-16 surrogate pairs for Unicode code points above U+FFFF, validates Unicode values, and converts them to the server's character encoding. It dynamically allocates memory for the output string and handles escape character doubling (where the escape character followed by itself represents a literal escape character).

## Parameters / Member Variables
- `*str`: Input string containing Unicode escape sequences to be processed
- `escape`: The escape character used in the string (typically backslash)
- `position`: Starting position of the U&'' or U&"" string token for error reporting
- `yyscanner`: Scanner context information needed for generating accurate error reports and positioning
## Dependencies
- Functions called/Symbols referenced:
  - [hexval](../h/hexval.md) (converts hexadecimal character to numeric value)
  - [check_unicode_value](../c/check_unicode_value.md) (validates Unicode code point)
  - [is_utf16_surrogate_first](../i/is_utf16_surrogate_first.md) (checks if code point is first half of surrogate pair)
  - [is_utf16_surrogate_second](../i/is_utf16_surrogate_second.md) (checks if code point is second half of surrogate pair)
  - [surrogate_pair_to_codepoint](surrogate_pair_to_codepoint.md) (combines surrogate pair into full Unicode code point)
  - [pg_unicode_to_server](../p/pg_unicode_to_server.md) (converts Unicode to server encoding)
  - setup_scanner_errposition_callback (sets up error positioning)
  - cancel_scanner_errposition_callback (cleans up error positioning)
  - [repalloc](../r/repalloc.md) (reallocates memory for growing output buffer)
- Called from (representative examples):
  - [base_yylex](../b/base_yylex.md) (PostgreSQL lexical analyzer at lines 286 and 302)

## Notes and Other Information
- The function uses dynamic memory allocation with initial size estimation and buffer expansion as needed
- Proper error handling includes detailed syntax error messages with cursor positioning
- UTF-16 surrogate pair validation ensures only valid Unicode sequences are processed
- The MAX_UNICODE_EQUIVALENT_STRING constant provides padding for Unicode-to-server encoding conversion
- Memory management uses PostgreSQL's palloc/repalloc functions for automatic cleanup on error
- Function is static and only used within the parser module for lexical analysis

## Simplified Source

```c
static char *str_udeescape(const char *str, char escape, int position, core_yyscan_t yyscanner) {
    const char *in = str;
    char *new, *out;
    size_t new_len = strlen(str) + MAX_UNICODE_EQUIVALENT_STRING + 1;
    pg_wchar pair_first = 0;

    new = palloc(new_len);
    out = new;

    while (*in) {
        // Expand buffer if needed
        if (out - new > new_len - (MAX_UNICODE_EQUIVALENT_STRING + 1)) {
            new_len *= 2;
            new = repalloc(new, new_len);
            out = new + (out - new);
        }

        if (in[0] == escape) {
            if (in[1] == escape) {
                // Doubled escape character = literal escape
                if (pair_first) goto invalid_pair;
                *out++ = escape;
                in += 2;
            } else if (isxdigit(in[1]) && isxdigit(in[2]) && isxdigit(in[3]) && isxdigit(in[4])) {
                // 4-digit Unicode escape: \XXXX
                pg_wchar unicode = (hexval(in[1]) << 12) + (hexval(in[2]) << 8) +
                                   (hexval(in[3]) << 4) + hexval(in[4]);
                check_unicode_value(unicode);

                // Handle UTF-16 surrogate pairs
                if (pair_first) {
                    if (is_utf16_surrogate_second(unicode)) {
                        unicode = surrogate_pair_to_codepoint(pair_first, unicode);
                        pair_first = 0;
                    } else {
                        goto invalid_pair;
                    }
                } else if (is_utf16_surrogate_second(unicode)) {
                    goto invalid_pair;
                }

                if (is_utf16_surrogate_first(unicode)) {
                    pair_first = unicode;
                } else {
                    pg_unicode_to_server(unicode, (unsigned char *) out);
                    out += strlen(out);
                }
                in += 5;
            } else if (in[1] == '+' && isxdigit(in[2]) && isxdigit(in[3]) &&
                       isxdigit(in[4]) && isxdigit(in[5]) && isxdigit(in[6]) && isxdigit(in[7])) {
                // 6-digit Unicode escape: \+XXXXXX
                pg_wchar unicode = (hexval(in[2]) << 20) + (hexval(in[3]) << 16) +
                                   (hexval(in[4]) << 12) + (hexval(in[5]) << 8) +
                                   (hexval(in[6]) << 4) + hexval(in[7]);
                check_unicode_value(unicode);

                // Handle surrogate pairs (same logic as 4-digit)
                if (pair_first) {
                    if (is_utf16_surrogate_second(unicode)) {
                        unicode = surrogate_pair_to_codepoint(pair_first, unicode);
                        pair_first = 0;
                    } else {
                        goto invalid_pair;
                    }
                } else if (is_utf16_surrogate_second(unicode)) {
                    goto invalid_pair;
                }

                if (is_utf16_surrogate_first(unicode)) {
                    pair_first = unicode;
                } else {
                    pg_unicode_to_server(unicode, (unsigned char *) out);
                    out += strlen(out);
                }
                in += 8;
            } else {
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("invalid Unicode escape"),
                         errhint("Unicode escapes must be \\XXXX or \\+XXXXXX.")));
            }
        } else {
            // Regular character
            if (pair_first) goto invalid_pair;
            *out++ = *in++;
        }
    }

    if (pair_first) goto invalid_pair;

    *out = '\0';
    return new;

invalid_pair:
    ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
             errmsg("invalid Unicode surrogate pair")));
    return NULL;
}
```