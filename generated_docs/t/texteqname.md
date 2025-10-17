# texteqname

## Location
[src/backend/utils/adt/varlena.c:2625-2649](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L2625-L2649)

## Overview
A cross-type equality comparison function that compares a text (variable-length string) with a name (fixed-length string) value for equality.

## Definition
Datum texteqname(PG_FUNCTION_ARGS)

## Detailed Description
This function implements cross-type equality comparison between PostgreSQL's text and name data types. It is the complement to nameeqtext, with the argument order reversed - the first argument is text and the second is name. Like its counterpart, it performs collation-aware comparison with an optimization for C collation using simple byte-wise comparison via memcmp. For other collations, it uses varstr_cmp to handle locale-specific comparison rules.

The function follows the same logic pattern as nameeqtext: check lengths for equality first, then perform the appropriate comparison based on the collation.

## Parameters / Member Variables
- arg1: Text value (variable-length string) retrieved via PG_GETARG_TEXT_PP(0)
- arg2: Name value (fixed-length string) retrieved via PG_GETARG_NAME(1)
- len1: Length of the text string (using VARSIZE_ANY_EXHDR)
- len2: Length of the name string (calculated using strlen)
- collid: Collation OID retrieved from the function context
- result: Boolean result of the equality comparison

## Dependencies
- Functions called/Symbols referenced:
  - Name (data type)
  - PG_GETARG_NAME
  - PG_GET_COLLATION
  - [check_collation_set](../c/check_collation_set.md)
  - [varstr_cmp](../v/varstr_cmp.md)
- Called from (representative examples):
  - No direct references found in the codebase (likely used through SQL equality operators)

## Notes and Other Information
- Complement function to nameeqtext with reversed argument order
- Optimizes for C collation by using direct memory comparison (memcmp)
- Uses varstr_cmp for non-C collations to handle locale-specific comparison rules
- Part of the cross-type comparison functions between text and name types
- The function properly handles memory management with PG_FREE_IF_COPY for the text argument
- Essential for SQL operations that compare user text values with system identifiers (names)
- Located in src/backend/utils/adt/varlena.c:2625-2649

## Simplified Source

```c
Datum
texteqname(PG_FUNCTION_ARGS)
{
    text *arg1 = PG_GETARG_TEXT_PP(0);
    Name arg2 = PG_GETARG_NAME(1);

    // Get lengths of both strings
    size_t len1 = VARSIZE_ANY_EXHDR(arg1);
    size_t len2 = strlen(NameStr(*arg2));

    Oid collid = PG_GET_COLLATION();
    check_collation_set(collid);

    bool result;

    // Fast path for C collation - simple byte comparison
    if (collid == C_COLLATION_OID) {
        result = (len1 == len2 &&
                  memcmp(VARDATA_ANY(arg1), NameStr(*arg2), len1) == 0);
    }
    else {
        // Locale-aware comparison for other collations
        result = (varstr_cmp(VARDATA_ANY(arg1), len1,
                           NameStr(*arg2), len2,
                           collid) == 0);
    }

    // Clean up potentially detoasted text value
    PG_FREE_IF_COPY(arg1, 0);

    PG_RETURN_BOOL(result);
}
```