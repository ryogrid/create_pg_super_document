# unicode_normalize_func

## Location
[src/backend/utils/adt/varlena.c:6344-6409](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L6344-L6409)

## Overview
Applies Unicode normalization to a UTF-8 text string using a specified normalization form (NFC, NFD, NFKC, or NFKD).

## Definition
```c
Datum unicode_normalize_func(PG_FUNCTION_ARGS)
```

## Detailed Description
The `unicode_normalize_func` function performs Unicode normalization on input text according to a specified normalization form. It takes two parameters: the input text to normalize and a string specifying the normalization form. The function converts the UTF-8 input to an internal wide character representation, applies the requested Unicode normalization algorithm, and then converts the result back to UTF-8 format. This function supports the standard Unicode normalization forms: NFC (Canonical Decomposition followed by Canonical Composition), NFD (Canonical Decomposition), NFKC (Compatibility Decomposition followed by Canonical Composition), and NFKD (Compatibility Decomposition).

## Parameters / Member Variables
- `input`: A `text*` parameter containing the UTF-8 encoded string to be normalized
- `formstr`: A `text*` parameter specifying the normalization form as a string (converted to C string internally)

## Dependencies
- Functions called/Symbols referenced:
  - [text_to_cstring](../t/text_to_cstring.md)
  - [unicode_norm_form_from_string](unicode_norm_form_from_string.md)
  - [pg_mbstrlen_with_len](../p/pg_mbstrlen_with_len.md)
  - [utf8_to_unicode](utf8_to_unicode.md)
  - [pg_utf_mblen](../p/pg_utf_mblen.md)
  - [unicode_normalize](unicode_normalize.md)
  - [unicode_to_utf8](unicode_to_utf8.md)
  - [palloc](../p/palloc.md)
  - SET_VARSIZE
  - PG_RETURN_TEXT_P
- Types referenced:
  - UnicodeNormalizationForm
- Called from:
  - No direct callers found (likely used as a SQL function)

## Notes and Other Information
- The function performs extensive UTF-8 to Unicode code point conversions and back
- Memory allocation is handled through PostgreSQL's memory management system (palloc)
- The function includes assertions to verify correct buffer boundary calculations
- Two-pass approach: first pass calculates output size, second pass fills the result buffer
- The function assumes UTF-8 encoding for both input and output text
- Supports all standard Unicode normalization forms through the unicode_norm_form_from_string parser

## Simplified Source

```c
Datum unicode_normalize_func(PG_FUNCTION_ARGS) {
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

    // Apply Unicode normalization
    pg_wchar *output_chars = unicode_normalize(form, input_chars);

    // Calculate output size by converting back to UTF-8
    int output_size = 0;
    for (pg_wchar *wp = output_chars; *wp; wp++) {
        unsigned char buf[4];
        unicode_to_utf8(*wp, buf);
        output_size += pg_utf_mblen(buf);
    }

    // Create result buffer and convert normalized characters to UTF-8
    text *result = palloc(output_size + VARHDRSZ);
    SET_VARSIZE(result, output_size + VARHDRSZ);

    p = (unsigned char *) VARDATA_ANY(result);
    for (pg_wchar *wp = output_chars; *wp; wp++) {
        unicode_to_utf8(*wp, p);
        p += pg_utf_mblen(p);
    }

    PG_RETURN_TEXT_P(result);
}
```