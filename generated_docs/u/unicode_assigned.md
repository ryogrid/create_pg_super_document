# unicode_assigned

## Location
[src/backend/utils/adt/varlena.c:6316-6343](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L6316-L6343)

## Overview
Checks whether a UTF-8 encoded string contains only assigned Unicode code points, returning false if any unassigned code points are found.

## Definition
```c
Datum unicode_assigned(PG_FUNCTION_ARGS)
```

## Detailed Description
The `unicode_assigned` function validates that all Unicode code points in a given text string are assigned (not unassigned) according to the Unicode standard. The function requires that the database encoding is UTF-8, as it specifically works with UTF-8 encoded strings. It iterates through each character in the input text, converts each UTF-8 sequence to a Unicode code point, determines the Unicode category of each code point, and returns false immediately if any code point belongs to the `PG_U_UNASSIGNED` category. If all code points are assigned, the function returns true.

## Parameters / Member Variables
- `input`: A `text*` parameter containing the UTF-8 encoded string to be validated for assigned Unicode code points

## Dependencies
- Functions called/Symbols referenced:
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md)
  - [pg_mbstrlen_with_len](../p/pg_mbstrlen_with_len.md)
  - [utf8_to_unicode](utf8_to_unicode.md)
  - [unicode_category](unicode_category.md)
  - [pg_utf_mblen](../p/pg_utf_mblen.md)
- Constants referenced:
  - PG_UTF8
  - PG_U_UNASSIGNED
- Called from:
  - No direct callers found (likely used as a SQL function)

## Notes and Other Information
- This function enforces UTF-8 database encoding requirement and will raise an error if called with other encodings
- The function is designed to be used as a PostgreSQL SQL function for Unicode validation
- It processes multi-byte UTF-8 sequences correctly by advancing the pointer by the appropriate number of bytes for each character
- Returns immediately upon finding the first unassigned code point for efficiency

## Simplified Source

```c
Datum unicode_assigned(PG_FUNCTION_ARGS) {
    text *input = PG_GETARG_TEXT_PP(0);

    // Ensure database is UTF8 encoded
    if (GetDatabaseEncoding() != PG_UTF8) {
        ereport(ERROR, (errmsg("Unicode categorization can only be performed if server encoding is UTF8")));
    }

    // Process each UTF-8 character in the input string
    int size = pg_mbstrlen_with_len(VARDATA_ANY(input), VARSIZE_ANY_EXHDR(input));
    unsigned char *p = (unsigned char *) VARDATA_ANY(input);

    for (int i = 0; i < size; i++) {
        // Convert UTF-8 sequence to Unicode code point
        pg_wchar uchar = utf8_to_unicode(p);
        int category = unicode_category(uchar);

        // Return false immediately if any unassigned code point found
        if (category == PG_U_UNASSIGNED)
            PG_RETURN_BOOL(false);

        // Advance to next UTF-8 character
        p += pg_utf_mblen(p);
    }

    // All code points are assigned
    PG_RETURN_BOOL(true);
}
```