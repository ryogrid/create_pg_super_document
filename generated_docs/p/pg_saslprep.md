# pg_saslprep

## Location
[src/common/saslprep.c:1049-1252](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/saslprep.c#L1049-L1252)

## Overview
The main function implementing SASLprep string normalization according to RFC 3454, used to normalize passwords and usernames for SASL authentication in PostgreSQL.

## Definition
```c
pg_saslprep_rc pg_saslprep(const char *input, char **output)
```

## Detailed Description
This function implements the complete SASLprep algorithm as specified in RFC 3454 for normalizing Unicode strings used in SASL authentication. SASLprep is essential for ensuring that passwords and usernames are processed consistently across different systems and locales.

The function follows the four-step SASLprep process:
1. **Mapping** - Maps certain characters to standard forms (e.g., non-ASCII spaces to ASCII space, some characters to nothing)
2. **Normalization** - Applies Unicode NFKC (Normalization Form KC) to handle character composition and compatibility
3. **Prohibition** - Rejects strings containing prohibited characters that could cause security issues
4. **Bidirectional checking** - Validates proper handling of right-to-left text according to Unicode bidirectional algorithm

The function includes an optimization for pure ASCII input (no processing needed) and comprehensive error handling for invalid UTF-8, prohibited characters, and memory allocation failures.

## Parameters / Member Variables
- `input`: Null-terminated UTF-8 encoded input string to normalize
- `output`: Pointer to receive the malloc'd/palloc'd normalized result string (set to NULL on failure)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_utf8_string_len](pg_utf8_string_len.md) (UTF-8 validation and length calculation)
  - [pg_is_ascii](pg_is_ascii.md) (ASCII optimization check)
  - [utf8_to_unicode](../u/utf8_to_unicode.md) (UTF-8 to Unicode codepoint conversion)
  - [unicode_normalize](../u/unicode_normalize.md) (Unicode NFKC normalization)
  - [unicode_to_utf8](../u/unicode_to_utf8.md) (Unicode codepoint to UTF-8 conversion)
  - [pg_utf_mblen](pg_utf_mblen.md) (UTF-8 multibyte length calculation)
  - IS_CODE_IN_TABLE (codepoint range checking macro)
  - Memory allocation/deallocation macros (ALLOC, FREE, STRDUP)
- Called from (representative examples):
  - [pg_be_scram_build_secret](pg_be_scram_build_secret.md) (backend SCRAM authentication)
  - [scram_verify_plain_password](../s/scram_verify_plain_password.md) (password verification)
  - [pg_fe_scram_build_secret](pg_fe_scram_build_secret.md) (frontend SCRAM authentication)
  - [scram_init](../s/scram_init.md) (SCRAM initialization)

## Notes and Other Information
- Returns pg_saslprep_rc enum values: SASLPREP_SUCCESS, SASLPREP_INVALID_UTF8, SASLPREP_PROHIBITED, or SASLPREP_OOM
- Critical component of PostgreSQL's SASL/SCRAM authentication system
- Handles memory management differently in frontend (malloc) vs backend (palloc)
- Implements comprehensive Unicode bidirectional text validation
- Empty passwords are explicitly prohibited after mapping
- Includes multiple Unicode character classification tables for prohibited, unassigned, and bidirectional character checking
- Essential for password security and authentication consistency across different Unicode representations

## Simplified Source

```c
pg_saslprep_rc
pg_saslprep(const char *input, char **output) {
    pg_wchar *input_chars = NULL;
    pg_wchar *output_chars = NULL;
    int input_size;
    char *result;
    int result_size;
    int count, i;
    bool contains_RandALCat;
    unsigned char *p;
    pg_wchar *wp;

    *output = NULL;

    // Quick ASCII optimization - no processing needed
    if (pg_is_ascii(input)) {
        *output = STRDUP(input);
        if (!(*output))
            goto oom;
        return SASLPREP_SUCCESS;
    }

    // Convert UTF-8 input to Unicode codepoints
    input_size = pg_utf8_string_len(input);
    if (input_size < 0)
        return SASLPREP_INVALID_UTF8;
    if (input_size >= MaxAllocSize / sizeof(pg_wchar))
        goto oom;

    input_chars = ALLOC((input_size + 1) * sizeof(pg_wchar));
    if (!input_chars)
        goto oom;

    // Convert to Unicode codepoints
    p = (unsigned char *) input;
    for (i = 0; i < input_size; i++) {
        input_chars[i] = utf8_to_unicode(p);
        p += pg_utf_mblen(p);
    }
    input_chars[i] = (pg_wchar) '\0';

    // Step 1: Character mapping
    count = 0;
    for (i = 0; i < input_size; i++) {
        pg_wchar code = input_chars[i];

        if (IS_CODE_IN_TABLE(code, non_ascii_space_ranges))
            input_chars[count++] = 0x0020;  // Map to ASCII space
        else if (IS_CODE_IN_TABLE(code, commonly_mapped_to_nothing_ranges))
            /* map to nothing - skip */;
        else
            input_chars[count++] = code;
    }
    input_chars[count] = (pg_wchar) '\0';
    input_size = count;

    if (input_size == 0)
        goto prohibited;  // Empty password not allowed

    // Step 2: Unicode normalization (NFKC)
    output_chars = unicode_normalize(UNICODE_NFKC, input_chars);
    if (!output_chars)
        goto oom;

    // Step 3: Check for prohibited characters
    for (i = 0; i < input_size; i++) {
        pg_wchar code = input_chars[i];
        if (IS_CODE_IN_TABLE(code, prohibited_output_ranges) ||
            IS_CODE_IN_TABLE(code, unassigned_codepoint_ranges))
            goto prohibited;
    }

    // Step 4: Bidirectional text validation
    contains_RandALCat = false;
    for (i = 0; i < input_size; i++) {
        if (IS_CODE_IN_TABLE(input_chars[i], RandALCat_codepoint_ranges)) {
            contains_RandALCat = true;
            break;
        }
    }

    if (contains_RandALCat) {
        // Check bidirectional requirements
        for (i = 0; i < input_size; i++) {
            if (IS_CODE_IN_TABLE(input_chars[i], LCat_codepoint_ranges))
                goto prohibited;
        }
        // First and last characters must be RandALCat
        if (!IS_CODE_IN_TABLE(input_chars[0], RandALCat_codepoint_ranges) ||
            !IS_CODE_IN_TABLE(input_chars[input_size - 1], RandALCat_codepoint_ranges))
            goto prohibited;
    }

    // Convert result back to UTF-8
    result_size = 0;
    for (wp = output_chars; *wp; wp++) {
        unsigned char buf[4];
        unicode_to_utf8(*wp, buf);
        result_size += pg_utf_mblen(buf);
    }

    result = ALLOC(result_size + 1);
    if (!result)
        goto oom;

    p = (unsigned char *) result;
    for (wp = output_chars; *wp; wp++) {
        unicode_to_utf8(*wp, p);
        p += pg_utf_mblen(p);
    }
    *p = '\0';

    FREE(input_chars);
    FREE(output_chars);
    *output = result;
    return SASLPREP_SUCCESS;

prohibited:
    if (input_chars) FREE(input_chars);
    if (output_chars) FREE(output_chars);
    return SASLPREP_PROHIBITED;

oom:
    if (input_chars) FREE(input_chars);
    if (output_chars) FREE(output_chars);
    return SASLPREP_OOM;
}
```