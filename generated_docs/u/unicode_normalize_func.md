# unicode_normalize_func

## Location
src/backend/utils/adt/varlena.c: 6344 - 6409

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
  - text_to_cstring
  - unicode_norm_form_from_string
  - pg_mbstrlen_with_len
  - utf8_to_unicode
  - pg_utf_mblen
  - unicode_normalize
  - unicode_to_utf8
  - palloc
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