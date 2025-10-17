# byteapos

## Location
[src/backend/utils/adt/varlena.c:3165-3208](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L3165-L3208)

## Overview
A PostgreSQL function that finds and returns the position of a specified substring within a bytea value, implementing the SQL POSITION() function for binary string data types.

## Definition

```c
Datum
byteapos(PG_FUNCTION_ARGS)
```
## Detailed Description
The `byteapos` function implements the SQL standard POSITION() function for bytea data types. It searches for the first occurrence of a substring (pattern) within a target bytea string and returns its 1-based position. The function performs a byte-by-byte comparison using `memcmp` for exact binary matching. If the pattern is found, it returns the position where the match starts (1-based indexing). If no match is found, it returns 0. The function is cloned from the text version (`textpos`) and modified to work with binary data. Special handling is provided for empty patterns, which always return position 1 according to SQL standards.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: `bytea *t1` - The target bytea string to search within
  - Argument 1: `bytea *t2` - The pattern bytea string to search for

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BYTEA_PP (for extracting bytea arguments)
  - VARSIZE_ANY_EXHDR (for getting the data size excluding headers)
  - VARDATA_ANY (for getting pointer to the actual data)
  - memcmp (for binary comparison of byte sequences)
  - PG_RETURN_INT32 (for returning 32-bit integer result)
- Called from:
  - SQL POSITION() function invocations on bytea data

## Notes and Other Information
- Uses 1-based indexing for positions as per SQL standard
- Returns 0 when no match is found
- Returns 1 for empty pattern searches (SQL standard behavior)
- Performs exact binary matching using `memcmp`
- Cloned and adapted from the text-based `textpos` function
- Uses efficient linear search algorithm with early termination
- Handles binary data that may contain null bytes
- Located in src/backend/utils/adt/varlena.c:3165-3208

## Simplified Source

```c
Datum byteapos(PG_FUNCTION_ARGS)
{
    bytea *target = PG_GETARG_BYTEA_PP(0);    // String to search in
    bytea *pattern = PG_GETARG_BYTEA_PP(1);   // Pattern to find

    int target_len = VARSIZE_ANY_EXHDR(target);
    int pattern_len = VARSIZE_ANY_EXHDR(pattern);

    // Empty pattern always found at position 1 (SQL standard)
    if (pattern_len <= 0)
        PG_RETURN_INT32(1);

    char *target_data = VARDATA_ANY(target);
    char *pattern_data = VARDATA_ANY(pattern);

    // Search for pattern in target string
    int max_start = target_len - pattern_len;
    for (int pos = 0; pos <= max_start; pos++) {
        // Check if pattern matches at current position
        if (target_data[pos] == pattern_data[0] &&
            memcmp(target_data + pos, pattern_data, pattern_len) == 0) {
            PG_RETURN_INT32(pos + 1);  // Return 1-based position
        }
    }

    PG_RETURN_INT32(0);  // Pattern not found
}
```

**Key Points:**
- Implements SQL POSITION() function for bytea data types
- Returns 1-based position of first pattern occurrence, 0 if not found
- Empty patterns return position 1 per SQL standard
- Uses memcmp for exact binary matching (handles null bytes correctly)