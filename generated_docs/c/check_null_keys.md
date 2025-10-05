# check_null_keys

## Location
[src/backend/access/brin/brin.c:2290-2353](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin.c#L2290-L2353)

## Overview
Checks whether a BRIN range value satisfies NULL-related scan keys (IS NULL/IS NOT NULL predicates) during index scanning.

## Definition
```c
static bool check_null_keys(BrinValues *bval, ScanKey *nullkeys, int nnullkeys)
```

## Detailed Description
This function evaluates NULL-related scan conditions against a BRIN range value to determine if the range can potentially contain matching tuples. It handles three types of NULL conditions:

1. **IS NULL predicates**: Returns false if the range contains no NULLs (neither all nulls nor has nulls)
2. **IS NOT NULL predicates**: Returns false only if the range contains exclusively NULLs
3. **Regular operators with NULL values**: Returns false assuming all indexable operators are strict

The function is part of BRIN's range filtering logic, helping to skip ranges that cannot satisfy the query conditions based on NULL value distribution.

## Parameters / Member Variables
- `bval`: Pointer to BrinValues structure containing the range's null information (bv_allnulls, bv_hasnulls, bv_attno)
- `nullkeys`: Array of ScanKey structures representing NULL-related scan conditions
- `nnullkeys`: Number of NULL-related scan keys to process

## Dependencies
- Functions called/Symbols referenced:
  - [BrinValues](../B/BrinValues.md) (structure for BRIN range values)
  - ScanKey (structure for scan conditions)
  - SK_ISNULL (scan key flag)
  - SK_SEARCHNULL (scan key flag for IS NULL)
  - SK_SEARCHNOTNULL (scan key flag for IS NOT NULL)
- Called from (representative examples):
  - [bringetbitmap](../b/bringetbitmap.md) (BRIN index bitmap scan)

## Notes and Other Information
- This is a static function internal to the BRIN access method
- The function assumes all indexable operators are strict (return false for NULL inputs)
- Uses BRIN range metadata (bv_allnulls, bv_hasnulls) to make efficient filtering decisions
- Part of the query optimization strategy to avoid scanning ranges that cannot contain matching tuples

## Simplified Source
```c
static bool
check_null_keys(BrinValues *bval, ScanKey *nullkeys, int nnullkeys)
{
    int keyno;

    // Check each NULL-related scan key
    for (keyno = 0; keyno < nnullkeys; keyno++)
    {
        ScanKey key = nullkeys[keyno];

        Assert(key->sk_attno == bval->bv_attno);

        // Only handle IS NULL/IS NOT NULL tests
        if (!(key->sk_flags & SK_ISNULL))
            continue;

        if (key->sk_flags & SK_SEARCHNULL)
        {
            // IS NULL: range must have some NULLs to match
            if (!bval->bv_allnulls && !bval->bv_hasnulls)
                return false;
        }
        else if (key->sk_flags & SK_SEARCHNOTNULL)
        {
            // IS NOT NULL: can only skip ranges with only NULLs
            if (bval->bv_allnulls)
                return false;
        }
        else
        {
            // Regular operator with NULL - assume strict operator
            return false;
        }
    }

    return true;  // All NULL conditions satisfied
}
```