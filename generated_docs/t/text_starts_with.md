# text_starts_with

## Location
[src/backend/utils/adt/varlena.c:1791-1830](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L1791-L1830)

## Overview
A PostgreSQL function that implements the "starts with" operation for text data types, returning true if the first text argument starts with the second text argument as a prefix.

## Definition

```c
Datum
text_starts_with(PG_FUNCTION_ARGS)
```
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

## Simplified Source

```c
Datum text_starts_with(PG_FUNCTION_ARGS) {
    Datum arg1 = PG_GETARG_DATUM(0);  // Text to check (haystack)
    Datum arg2 = PG_GETARG_DATUM(1);  // Prefix to find (needle)
    Oid collid = PG_GET_COLLATION();

    // Validate collation and ensure deterministic behavior
    check_collation_set(collid);
    if (!lc_collate_is_c(collid)) {
        pg_locale_t mylocale = pg_newlocale_from_collation(collid);
        if (!pg_locale_deterministic(mylocale)) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("nondeterministic collations are not supported for substring searches")));
        }
    }

    // Quick length check - prefix can't be longer than target
    Size len1 = toast_raw_datum_size(arg1);
    Size len2 = toast_raw_datum_size(arg2);
    if (len2 > len1) {
        PG_RETURN_BOOL(false);
    }

    // Extract prefix-length substring and compare
    text *targ1 = text_substring(arg1, 1, len2, false);
    text *targ2 = DatumGetTextPP(arg2);

    bool result = (memcmp(VARDATA_ANY(targ1), VARDATA_ANY(targ2),
                         VARSIZE_ANY_EXHDR(targ2)) == 0);

    PG_FREE_IF_COPY(targ1, 0);
    PG_FREE_IF_COPY(targ2, 1);

    PG_RETURN_BOOL(result);
}
```