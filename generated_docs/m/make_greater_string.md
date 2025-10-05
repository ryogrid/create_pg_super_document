# make_greater_string

## Location
[src/backend/utils/adt/like_support.c:1573-1723](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like_support.c#L1573-L1723)

## Overview
Generates a string that is lexicographically greater than a given prefix string for pattern matching optimization in PostgreSQL's LIKE operations.

## Definition
```c
static Const *make_greater_string(const Const *str_const, FmgrInfo *ltproc, Oid collation)
```

## Detailed Description
This function attempts to create a string that is guaranteed to be greater than the input string and any string that has the input as a prefix. This is used in PostgreSQL's query optimization to create range bounds for index scans on LIKE patterns.

The algorithm works as follows:
1. For non-C collations, appends a suffix character (determined by comparing "Z", "z", "y", "9" to find the largest)
2. Repeatedly increments the rightmost character using encoding-specific increment functions
3. If incrementing fails, truncates the last character and tries incrementing the next character to the left
4. Continues until a valid greater string is found or the string is exhausted

The function handles different data types (text, name, bytea) and respects collation rules. For bytea, it uses simple byte-wise comparison. For text types in non-C collations, it uses special handling to account for complex sorting rules.

## Parameters / Member Variables
- `str_const`: Input Const node containing the string to increment
- `ltproc`: Function pointer for the "less than" comparison function
- `collation`: Oid specifying the collation to use for comparisons

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetByteaPP (bytea handling)
  - DirectFunctionCall1, DatumGetCString, TextDatumGetCString (string conversion)
  - [lc_collate_is_c](../l/lc_collate_is_c.md) (collation checking)
  - [varstr_cmp](../v/varstr_cmp.md) (string comparison for suffix determination)
  - [byte_increment](../b/byte_increment.md) (bytea character incrementing)
  - [pg_database_encoding_character_incrementer](../p/pg_database_encoding_character_incrementer.md) (text character incrementing)
  - [pg_mbcliplen](../p/pg_mbcliplen.md) (multibyte character handling)
  - [string_to_bytea_const](../s/string_to_bytea_const.md), string_to_const (result construction)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md) (collation-aware comparison)
- Called from (representative examples):
  - Pattern_Prefix_Status
  - [match_pattern_prefix](match_pattern_prefix.md)
  - [prefix_selectivity](../p/prefix_selectivity.md)

## Notes and Other Information
- This is a static function within like_support.c, used for LIKE pattern optimization
- The function includes important caveats about non-C collations where the result may not be absolutely reliable
- Uses a static cache for suffix character determination to avoid repeated collation comparisons
- Returns NULL if no greater string can be generated (e.g., for maximum-value strings)
- The algorithm is designed to be reasonably efficient, avoiding exhaustive character searches
- Critical for PostgreSQL's ability to convert LIKE patterns into index-scannable range conditions

## Simplified Source
```c
static Const *make_greater_string(const Const *str_const, FmgrInfo *ltproc, Oid collation) {
    Oid datatype = str_const->consttype;
    char *workstr;
    int len;
    Datum cmpstr;
    char *cmptxt = NULL;
    mbcharacter_incrementer charinc;

    // Extract string data based on type
    if (datatype == BYTEAOID) {
        // Handle bytea type: simple byte-wise operations
        bytea *bstr = DatumGetByteaPP(str_const->constvalue);
        len = VARSIZE_ANY_EXHDR(bstr);
        workstr = palloc(len);
        memcpy(workstr, VARDATA_ANY(bstr), len);
        cmpstr = str_const->constvalue;
    }
    else {
        // Handle text/name types
        if (datatype == NAMEOID)
            workstr = DatumGetCString(DirectFunctionCall1(nameout, str_const->constvalue));
        else
            workstr = TextDatumGetCString(str_const->constvalue);

        len = strlen(workstr);

        if (lc_collate_is_c(collation) || len == 0) {
            cmpstr = str_const->constvalue;
        }
        else {
            // For non-C collations, append suffix character
            static char suffixchar = 0;
            static Oid suffixcollation = 0;

            // Determine best suffix character for this collation
            if (!suffixchar || suffixcollation != collation) {
                char *best = "Z";
                if (varstr_cmp(best, 1, "z", 1, collation) < 0) best = "z";
                if (varstr_cmp(best, 1, "y", 1, collation) < 0) best = "y";
                if (varstr_cmp(best, 1, "9", 1, collation) < 0) best = "9";
                suffixchar = *best;
                suffixcollation = collation;
            }

            // Build comparison string with suffix
            // ... suffix appending logic simplified ...
        }
    }

    // Select character incrementer function
    if (datatype == BYTEAOID)
        charinc = byte_increment;
    else
        charinc = pg_database_encoding_character_incrementer();

    // Main algorithm: try to increment rightmost characters
    while (len > 0) {
        int charlen;
        unsigned char *lastchar;

        // Find last character (encoding-aware)
        if (datatype == BYTEAOID) {
            charlen = 1;
        } else {
            charlen = len - pg_mbcliplen(workstr, len, len - 1);
        }
        lastchar = (unsigned char *) (workstr + len - charlen);

        // Try incrementing the last character
        while (charinc(lastchar, charlen)) {
            Const *workstr_const;

            // Create candidate string
            if (datatype == BYTEAOID)
                workstr_const = string_to_bytea_const(workstr, len);
            else
                workstr_const = string_to_const(workstr, datatype);

            // Test if it's greater than comparison string
            if (DatumGetBool(FunctionCall2Coll(ltproc, collation, cmpstr, workstr_const->constvalue))) {
                // Success: found a greater string
                if (cmptxt) pfree(cmptxt);
                pfree(workstr);
                return workstr_const;
            }

            // Not greater, clean up and try next increment
            pfree(DatumGetPointer(workstr_const->constvalue));
            pfree(workstr_const);
        }

        // Can't increment last character, truncate and try previous
        len -= charlen;
        workstr[len] = '\0';
    }

    // Failed to find greater string
    if (cmptxt) pfree(cmptxt);
    pfree(workstr);
    return NULL;
}
```