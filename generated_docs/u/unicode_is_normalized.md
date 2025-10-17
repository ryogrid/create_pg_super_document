# unicode_is_normalized

## Location
[src/backend/utils/adt/varlena.c:6410-6461](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L6410-L6461)

## Overview
Checks whether a UTF-8 text string is already in a specified Unicode normalization form, using optimized quick-check algorithms when possible.

## Definition
```c
Datum unicode_is_normalized(PG_FUNCTION_ARGS)
```

## Detailed Description
The `unicode_is_normalized` function determines whether input text is already in the specified Unicode normalization form without necessarily performing full normalization. It implements an optimized approach using the "quick check" algorithm from Unicode Standard Annex #15 (UAX #15), which can often provide a definitive answer by scanning the string only once. If the quick check returns an inconclusive result (UNICODE_NORM_QC_MAYBE), the function falls back to performing full normalization and comparing the result with the original input. This function is optimized for the common case where strings are already normalized, avoiding unnecessary conversion overhead when possible.

## Parameters / Member Variables
- `input`: A `text*` parameter containing the UTF-8 encoded string to check for normalization
- `formstr`: A `text*` parameter specifying the normalization form as a string

## Dependencies
- Functions called/Symbols referenced:
  - [text_to_cstring](../t/text_to_cstring.md)
  - [unicode_norm_form_from_string](unicode_norm_form_from_string.md)
  - [pg_mbstrlen_with_len](../p/pg_mbstrlen_with_len.md)
  - [utf8_to_unicode](utf8_to_unicode.md)
  - [pg_utf_mblen](../p/pg_utf_mblen.md)
  - [unicode_is_normalized_quickcheck](unicode_is_normalized_quickcheck.md)
  - [unicode_normalize](unicode_normalize.md)
  - [palloc](../p/palloc.md)
  - memcmp
- Types referenced:
  - UnicodeNormalizationForm
  - [UnicodeNormalizationQC](../U/UnicodeNormalizationQC.md)
- Constants referenced:
  - UNICODE_NORM_QC_YES
  - UNICODE_NORM_QC_NO
- Called from:
  - No direct callers found (likely used as a SQL function)

## Notes and Other Information
- Implements the UAX #15 quick check algorithm for performance optimization
- Falls back to full normalization only when quick check returns inconclusive results
- Optimized for strings that are already in normalized form (the common case)
- Uses efficient memory comparison for final result determination when full normalization is required
- The quick check can return YES (definitely normalized), NO (definitely not normalized), or MAYBE (requires full check)
- Performs size comparison before memory comparison for early exit optimization

## Simplified Source

```c
Datum unicode_is_normalized(PG_FUNCTION_ARGS) {
    text *input = PG_GETARG_TEXT_PP(0);
    char *formstr = text_to_cstring(PG_GETARG_TEXT_PP(1));

    // Parse normalization form string (validates UTF8 encoding)
    UnicodeNormalizationForm form = unicode_norm_form_from_string(formstr);

    // Convert UTF-8 input to wide character array
    int size = pg_mbstrlen_with_len(VARDATA_ANY(input), VARSIZE_ANY_EXHDR(input));
    pg_wchar *input_chars = palloc((size + 1) * sizeof(pg_wchar));
    unsigned char *p = (unsigned char *) VARDATA_ANY(input);

    for (int i = 0; i < size; i++) {
        input_chars[i] = utf8_to_unicode(p);
        p += pg_utf_mblen(p);
    }
    input_chars[size] = (pg_wchar) '\0';

    // Quick check algorithm from UAX #15 - can often give definitive answer
    UnicodeNormalizationQC quickcheck = unicode_is_normalized_quickcheck(form, input_chars);
    if (quickcheck == UNICODE_NORM_QC_YES)
        PG_RETURN_BOOL(true);
    else if (quickcheck == UNICODE_NORM_QC_NO)
        PG_RETURN_BOOL(false);

    // Quick check inconclusive - perform full normalization and compare
    pg_wchar *output_chars = unicode_normalize(form, input_chars);

    // Count output characters
    int output_size = 0;
    for (pg_wchar *wp = output_chars; *wp; wp++)
        output_size++;

    // Compare sizes and contents
    bool result = (size == output_size) &&
                  (memcmp(input_chars, output_chars, size * sizeof(pg_wchar)) == 0);

    PG_RETURN_BOOL(result);
}
```