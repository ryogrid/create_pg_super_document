# text_starts_with

## Location
src/backend/utils/adt/varlena.c: 1791 - 1830

## Overview
A PostgreSQL function that implements the "starts with" operation for text data types, returning true if the first text argument starts with the second text argument as a prefix.

## Definition


## Detailed Description
The `text_starts_with` function is a PostgreSQL built-in function that determines whether one text value starts with another text value as a prefix. The function performs collation validation and handles locale-specific considerations, ensuring deterministic behavior by rejecting nondeterministic collations. It uses an efficient approach by extracting a substring from the first argument that matches the length of the second argument, then performs a byte-level comparison using `memcmp`. This implementation optimizes performance by avoiding full string comparison when the prefix length exceeds the target string length.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: PostgreSQL's standard macro for function arguments, containing:
  - `arg1` (Datum): The text value to check for the prefix (haystack)
  - `arg2` (Datum): The text value to use as the prefix to search for (needle)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GET_COLLATION`: Retrieves the collation to use for the operation
  - [check_collation_set](../c/check_collation_set.md): Validates that a collation is properly set
  - [lc_collate_is_c](../l/lc_collate_is_c.md): Checks if the collation is the C locale
  - [pg_newlocale_from_collation](../p/pg_newlocale_from_collation.md): Creates a locale object from collation OID
  - [pg_locale_deterministic](../p/pg_locale_deterministic.md): Checks if the locale provides deterministic sorting
  - [toast_raw_datum_size](toast_raw_datum_size.md): Gets the size of a potentially-toasted datum
  - [text_substring](text_substring.md): Extracts a substring from a text value
  - `DatumGetTextPP`: Converts a Datum to text pointer with possible detoasting
  - `VARDATA_ANY`: Macro to get pointer to variable-length data
  - `VARSIZE_ANY_EXHDR`: Macro to get size of variable-length data excluding header
  - `PG_FREE_IF_COPY`: Memory management macro to free copied arguments if necessary
  - `PG_RETURN_BOOL`: Macro to return boolean result as Datum
- Called from (representative examples):
  - [spg_text_leaf_consistent](../s/spg_text_leaf_consistent.md): Used in SP-GiST index operations for text prefix matching

## Notes and Other Information
- This function is used to implement prefix matching operations in PostgreSQL
- Rejects nondeterministic collations with an error, ensuring consistent results
- Uses efficient size-based early exit when the prefix is longer than the target string
- Performs byte-level comparison after substring extraction for optimal performance
- Properly handles TOAST (The Oversized-Attribute Storage Technique) for large text values
- Part of PostgreSQL's text processing capabilities, particularly useful for pattern matching and indexing
- The function is defined in `src/backend/utils/adt/varlena.c` at lines 1791-1830