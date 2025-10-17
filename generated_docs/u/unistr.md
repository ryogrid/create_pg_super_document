# unistr

## Location
[src/backend/utils/adt/varlena.c:6502-6667](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L6502-L6667)

## Overview
A PostgreSQL built-in function that processes Unicode escape sequences in text strings and converts them to their corresponding Unicode characters in the server encoding.

## Definition
```c
Datum unistr(PG_FUNCTION_ARGS)
```

## Detailed Description
The `unistr` function implements PostgreSQL's Unicode string processing functionality, similar to Oracle's UNISTR function. It scans through an input text string and replaces Unicode escape sequences with their corresponding Unicode characters. The function supports multiple Unicode escape sequence formats:

1. `\\XXXX` - 4-digit hexadecimal Unicode code point
2. `\\uXXXX` - 4-digit hexadecimal Unicode code point (with 'u' prefix)
3. `\\+XXXXXX` - 6-digit hexadecimal Unicode code point (with '+' prefix)
4. `\\UXXXXXXXX` - 8-digit hexadecimal Unicode code point (with 'U' prefix)

The function properly handles UTF-16 surrogate pairs for characters beyond the Basic Multilingual Plane (BMP), validates Unicode code points, and converts the results to the server's character encoding. It also handles escaped backslashes (`\\\\`) by converting them to single backslashes.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [hexval_n](../h/hexval_n.md): Converts hexadecimal digit sequences to numeric values
  - [isxdigits_n](../i/isxdigits_n.md): Validates that a sequence contains valid hexadecimal digits
  - `[is_valid_unicode_codepoint](../i/is_valid_unicode_codepoint.md)`: Validates Unicode code point values
  - `[is_utf16_surrogate_first](../i/is_utf16_surrogate_first.md)/second`: Handles UTF-16 surrogate pair validation
  - `[surrogate_pair_to_codepoint](../s/surrogate_pair_to_codepoint.md)`: Combines UTF-16 surrogate pairs into code points
  - [pg_unicode_to_server](../p/pg_unicode_to_server.md): Converts Unicode to server encoding
  - `[cstring_to_text_with_len](../c/cstring_to_text_with_len.md)`: Creates PostgreSQL text result
  - `MAX_UNICODE_EQUIVALENT_STRING`: Buffer size constant
- Called from (representative examples):
  - No direct callers found (likely called through PostgreSQL's function call mechanism as a SQL function)

## Notes and Other Information
- This is a PostgreSQL built-in function available to SQL users
- Supports proper UTF-16 surrogate pair handling for characters beyond the BMP (U+10000 and above)
- Validates all Unicode escape sequences and throws errors for invalid formats or code points
- Handles incomplete surrogate pairs by reporting syntax errors
- The function maintains state to track surrogate pair processing across multiple escape sequences
- All Unicode characters are converted to the server's character encoding
- Located at src/backend/utils/adt/varlena.c:6502-6667
- Error messages provide helpful hints about valid Unicode escape sequence formats

## Simplified Source

```c
Datum unistr(PG_FUNCTION_ARGS) {
    text *input_text = PG_GETARG_TEXT_PP(0);
    char *instr = VARDATA_ANY(input_text);
    int len = VARSIZE_ANY_EXHDR(input_text);
    StringInfoData str;
    pg_wchar pair_first = 0;  // Track UTF-16 surrogate pairs
    char cbuf[MAX_UNICODE_EQUIVALENT_STRING + 1];

    initStringInfo(&str);

    while (len > 0) {
        if (instr[0] == '\\') {
            // Handle escaped backslash
            if (len >= 2 && instr[1] == '\\') {
                if (pair_first) goto invalid_pair;
                appendStringInfoChar(&str, '\\');
                instr += 2; len -= 2;
            }
            // Handle Unicode escape sequences: \XXXX, \uXXXX, \+XXXXXX, \UXXXXXXXX
            else if (/* various escape formats */) {
                pg_wchar unicode = /* parse hex digits */;

                // Validate Unicode code point
                if (!is_valid_unicode_codepoint(unicode))
                    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("invalid Unicode code point: %04X", unicode)));

                // Handle UTF-16 surrogate pairs for characters > U+FFFF
                if (pair_first) {
                    if (is_utf16_surrogate_second(unicode)) {
                        unicode = surrogate_pair_to_codepoint(pair_first, unicode);
                        pair_first = 0;
                    } else goto invalid_pair;
                } else if (is_utf16_surrogate_second(unicode)) {
                    goto invalid_pair;
                }

                // Store first surrogate or convert to server encoding
                if (is_utf16_surrogate_first(unicode)) {
                    pair_first = unicode;
                } else {
                    pg_unicode_to_server(unicode, (unsigned char *) cbuf);
                    appendStringInfoString(&str, cbuf);
                }

                /* advance past escape sequence */
            }
            else {
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("invalid Unicode escape")));
            }
        } else {
            // Regular character - copy directly
            if (pair_first) goto invalid_pair;
            appendStringInfoChar(&str, *instr++);
            len--;
        }
    }

    // Check for incomplete surrogate pair
    if (pair_first) goto invalid_pair;

    text *result = cstring_to_text_with_len(str.data, str.len);
    pfree(str.data);
    PG_RETURN_TEXT_P(result);

invalid_pair:
    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
            errmsg("invalid Unicode surrogate pair")));
}
```