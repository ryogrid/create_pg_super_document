# namenetext

## Location
[src/backend/utils/adt/varlena.c:2650-2674](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L2650-L2674)

## Overview
The  function implements the not-equal comparison operator between a name type and a text type in PostgreSQL.

## Definition

```c
Datum
namenetext(PG_FUNCTION_ARGS)
```
## Detailed Description
This function compares a name (fixed-length string) with a text value and returns true if they are not equal. It handles both C collation (simple byte comparison) and locale-aware collation using PostgreSQL's collation system. The function first extracts the lengths of both arguments, then performs either a direct memory comparison for C collation or uses the  function for locale-aware comparison.

## Parameters / Member Variables
- : Name type argument (extracted using )
- : Text type argument (extracted using )

## Dependencies
- Functions called/Symbols referenced:
  - : Extract name argument
  - : Extract text argument with possible detoasting
  - : Get collation for comparison
  - : Validate collation is set
  - : Perform locale-aware string comparison
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/varlena.c:2650-2674
- Uses efficient memory comparison for C collation
- Properly handles variable-length text data with detoasting
- Returns the negation of equality comparison (not-equal operation)
- Frees copied text argument to prevent memory leaks

## Simplified Source

```c
Datum
namenetext(PG_FUNCTION_ARGS)
{
    Name arg1 = PG_GETARG_NAME(0);
    text *arg2 = PG_GETARG_TEXT_PP(1);

    // Get lengths of both strings
    size_t len1 = strlen(NameStr(*arg1));
    size_t len2 = VARSIZE_ANY_EXHDR(arg2);

    Oid collid = PG_GET_COLLATION();
    check_collation_set(collid);

    bool result;

    // Fast path for C collation - simple byte comparison
    if (collid == C_COLLATION_OID) {
        result = !(len1 == len2 &&
                   memcmp(NameStr(*arg1), VARDATA_ANY(arg2), len1) == 0);
    }
    else {
        // Locale-aware comparison for other collations
        result = !(varstr_cmp(NameStr(*arg1), len1,
                            VARDATA_ANY(arg2), len2,
                            collid) == 0);
    }

    // Clean up potentially detoasted text value
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL(result);
}
```