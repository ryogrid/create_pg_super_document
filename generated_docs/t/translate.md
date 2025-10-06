# translate

## Location
[src/backend/utils/adt/oracle_compat.c:797-924](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/oracle_compat.c#L797-L924)

## Overview
A string manipulation function that replaces characters in a text string based on mapping rules defined by two character sets.

## Definition
```c
Datum translate(PG_FUNCTION_ARGS)
```

## Detailed Description
The translate function implements character-by-character replacement within a text string. It takes three parameters: the source string, a 'from' character set, and a 'to' character set. For each character in the source string, if it matches any character in the 'from' set, it gets replaced with the corresponding character from the 'to' set at the same position. If the 'from' set is longer than the 'to' set, excess characters in 'from' will cause matching characters to be deleted from the result. This function handles multibyte character encodings correctly and includes overflow protection for memory allocation.

## Parameters / Member Variables
- `string`: The source text string to be processed
- `from`: Character set defining which characters to replace
- `to`: Character set defining replacement characters

## Dependencies
- Functions called/Symbols referenced:
  - PG_RETURN_TEXT_P
  - [pg_database_encoding_max_length](../p/pg_database_encoding_max_length.md)
  - [pg_mul_s32_overflow](../p/pg_mul_s32_overflow.md)
  - [pg_add_s32_overflow](../p/pg_add_s32_overflow.md)
  - AllocSizeIsValid
  - VARDATA
  - [pg_mblen](../p/pg_mblen.md)
  - SET_VARSIZE
- Called from (representative examples):
  - [printTableAddHeader](../p/printTableAddHeader.md) (src/fe_utils/print.c)
  - [printTableAddCell](../p/printTableAddCell.md) (src/fe_utils/print.c)
  - [printQuery](../p/printQuery.md) (src/fe_utils/print.c)

## Notes and Other Information
- Located in src/backend/utils/adt/oracle_compat.c:797-924
- Part of PostgreSQL's Oracle compatibility layer
- Handles multibyte character encodings with proper character boundary detection
- Includes sophisticated memory overflow protection using safe arithmetic functions
- The algorithm processes characters sequentially, maintaining character encoding integrity
- Improved implementation credited to Edwin Ramirez <ramirez@doc.mssm.edu>
- If replacement results in shorter strings, memory allocation may be larger than needed, but this is deemed acceptable for performance reasons

## Simplified Source

```c
Datum
translate(PG_FUNCTION_ARGS)
{
    // Extract input arguments
    text *string = PG_GETARG_TEXT_PP(0);
    text *from = PG_GETARG_TEXT_PP(1);
    text *to = PG_GETARG_TEXT_PP(2);
    text *result;

    // Get string data and lengths
    char *source = VARDATA_ANY(string);
    int m = VARSIZE_ANY_EXHDR(string);
    int fromlen = VARSIZE_ANY_EXHDR(from);
    int tolen = VARSIZE_ANY_EXHDR(to);
    char *from_ptr = VARDATA_ANY(from);
    char *to_ptr = VARDATA_ANY(to);

    // Handle empty string case
    if (m <= 0)
        PG_RETURN_TEXT_P(string);

    // Allocate worst-case result buffer with overflow protection
    int bytelen = pg_database_encoding_max_length() * m + VARHDRSZ;
    result = (text *) palloc(bytelen);
    char *target = VARDATA(result);
    int retlen = 0;

    // Process each character in source string
    while (m > 0) {
        int source_len = pg_mblen(source);
        int from_index = 0;
        bool found = false;

        // Search for character in 'from' set
        for (int i = 0; i < fromlen; i += pg_mblen(&from_ptr[i])) {
            if (pg_mblen(&from_ptr[i]) == source_len &&
                memcmp(source, &from_ptr[i], source_len) == 0) {
                found = true;
                break;
            }
            from_index++;
        }

        if (found) {
            // Find corresponding 'to' character and substitute/delete
            char *p = to_ptr;
            for (int i = 0; i < from_index && p < to_ptr + tolen; i++)
                p += pg_mblen(p);

            if (p < to_ptr + tolen) {
                int len = pg_mblen(p);
                memcpy(target, p, len);
                target += len;
                retlen += len;
            }
        } else {
            // No match found, copy original character
            memcpy(target, source, source_len);
            target += source_len;
            retlen += source_len;
        }

        source += source_len;
        m -= source_len;
    }

    SET_VARSIZE(result, retlen + VARHDRSZ);
    PG_RETURN_TEXT_P(result);
}
```