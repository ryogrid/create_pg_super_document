# unicode_norm_form_from_string

## Location
[src/backend/utils/adt/varlena.c:6256-6292](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L6256-L6292)

## Overview
Converts a string representation of a Unicode normalization form into the corresponding `UnicodeNormalizationForm` enumeration value, with validation for database encoding compatibility.

## Definition
```c
static UnicodeNormalizationForm unicode_norm_form_from_string(const char *formstr)
```

## Detailed Description
This static function parses a string representation of Unicode normalization forms and returns the corresponding enumeration value. It supports the four standard Unicode normalization forms (NFC, NFD, NFKC, NFKD) through case-insensitive string comparison.

The function includes important validation:
- Ensures the database encoding is UTF8, as Unicode normalization operations are only meaningful for UTF8 encoded text
- Validates that the provided form string matches one of the supported normalization forms
- Raises appropriate errors for invalid encoding or unrecognized normalization forms

## Parameters / Member Variables
- `formstr`: String representation of the desired Unicode normalization form (case-insensitive)

## Dependencies
- Functions called/Symbols referenced:
  - `UnicodeNormalizationForm` (enum type)
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md)() (encoding validation function)
  - `PG_UTF8` (UTF8 encoding constant)
  - `UNICODE_NFC`, `UNICODE_NFD`, `UNICODE_NFKC`, `UNICODE_NFKD` (normalization form constants)
  - [pg_strcasecmp](../p/pg_strcasecmp.md)() (case-insensitive string comparison)
  - `ereport()` (error reporting function)
- Called from (representative examples):
  - [unicode_normalize_func](unicode_normalize_func.md) in src/backend/utils/adt/varlena.c:6356
  - [unicode_is_normalized](unicode_is_normalized.md) in src/backend/utils/adt/varlena.c:6424

## Notes and Other Information
- Only works when the database encoding is UTF8; raises an error for other encodings
- Supports four Unicode normalization forms:
  - NFC (Canonical Decomposition, followed by Canonical Composition)
  - NFD (Canonical Decomposition)
  - NFKC (Compatibility Decomposition, followed by Canonical Composition)
  - NFKD (Compatibility Decomposition)
- String comparison is case-insensitive using `pg_strcasecmp()`
- Raises `ERRCODE_SYNTAX_ERROR` for encoding issues and `ERRCODE_INVALID_PARAMETER_VALUE` for invalid form strings
- This function is part of PostgreSQL\s Unicode text processing support