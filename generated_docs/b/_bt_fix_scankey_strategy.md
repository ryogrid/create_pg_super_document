# _bt_fix_scankey_strategy

## Location
[src/backend/access/nbtree/nbtutils.c:3329-3437](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L3329-L3437)

## Overview
Adjusts a scankey's strategy and flags to account for index options like DESC ordering and handles IS NULL/NOT NULL conditions while validating comparison values.

## Definition
static bool _bt_fix_scankey_strategy(ScanKey skey, int16 *indoption)

## Detailed Description
This function performs several critical adjustments to scan keys during B-tree preprocessing:

1. **Index option handling**: Copies appropriate indoption values into sk_flags and commutes (flips) operator strategies for DESC columns.

2. **IS NULL/NOT NULL processing**: Sets up correct strategy numbers for null search conditions:
   - IS NULL becomes BTEqualStrategyNumber
   - IS NOT NULL becomes BTGreaterStrategyNumber (NULLS FIRST) or BTLessStrategyNumber (NULLS LAST)

3. **NULL value validation**: Checks for NULL comparison values in regular operators and returns false if found, since all btree operators are treated as strict.

4. **Row scankey handling**: Recursively processes SK_ROW_HEADER scankeys by applying the same transformations to all SK_ROW_MEMBER subkeys.

The function must handle rescan scenarios carefully to avoid re-commuting strategies that were already processed.

## Parameters / Member Variables
- : ScanKey - The scan key to adjust (modified in-place)
- : int16 * - Array of index options per column containing DESC, NULLS FIRST/LAST flags

## Dependencies
- Functions called/Symbols referenced:
  - BTCommuteStrategyNumber (strategy flipping for DESC columns)
  - BTEqualStrategyNumber (strategy for IS NULL)
  - BTGreaterStrategyNumber (strategy for IS NOT NULL in NULLS FIRST)
  - BTLessStrategyNumber (strategy for IS NOT NULL in NULLS LAST)
  - InvalidOid (clearing subtype and collation for NULL searches)
  - InvalidStrategy (checking eliminated array keys)
  - Various scan key flags (SK_ISNULL, SK_SEARCHNULL, SK_SEARCHNOTNULL, SK_BT_DESC, SK_ROW_HEADER, SK_ROW_MEMBER, SK_ROW_END, SK_SEARCHARRAY)
  - SK_BT_INDOPTION_SHIFT (bit shifting for flag storage)

- Called from (representative examples):
  - [_bt_preprocess_keys](_bt_preprocess_keys.md) (during scan key preprocessing phase)

## Notes and Other Information
- Returns true if the scan key is valid and should be used, false if the scan should be abandoned
- Modifies the caller's scankey structure in-place, which persists across rescans
- Treats all btree operators as strict, rejecting NULL comparison values for regular operators
- Must be careful not to re-commute strategies on rescan (checks SK_BT_DESC flag)
- Handles both simple scankeys and complex row-comparison scankeys with subkey processing
- Part of PostgreSQL's index option and NULL handling infrastructure
- The SK_BT_INDOPTION_SHIFT ensures index options don't conflict with system-defined flag bits

## Simplified Source

```c
static bool
_bt_fix_scankey_strategy(ScanKey skey, int16 *indoption)
{
    int addflags;

    addflags = indoption[skey->sk_attno - 1] << SK_BT_INDOPTION_SHIFT;

    // Handle NULL scan keys (IS NULL/NOT NULL conditions)
    if (skey->sk_flags & SK_ISNULL)
    {
        skey->sk_flags |= addflags;

        if (skey->sk_flags & SK_SEARCHNULL)
        {
            // IS NULL becomes equality search
            skey->sk_strategy = BTEqualStrategyNumber;
            skey->sk_subtype = InvalidOid;
            skey->sk_collation = InvalidOid;
        }
        else if (skey->sk_flags & SK_SEARCHNOTNULL)
        {
            // IS NOT NULL becomes inequality based on null ordering
            if (skey->sk_flags & SK_BT_NULLS_FIRST)
                skey->sk_strategy = BTGreaterStrategyNumber;
            else
                skey->sk_strategy = BTLessStrategyNumber;
            skey->sk_subtype = InvalidOid;
            skey->sk_collation = InvalidOid;
        }
        else
        {
            // NULL comparison value for regular operator - invalid
            return false;
        }

        return true;
    }

    // Skip already-eliminated array keys
    if (skey->sk_strategy == InvalidStrategy)
        return true;

    // Apply DESC column transformation (commute strategy)
    if ((addflags & SK_BT_DESC) && !(skey->sk_flags & SK_BT_DESC))
        skey->sk_strategy = BTCommuteStrategyNumber(skey->sk_strategy);
    skey->sk_flags |= addflags;

    // Handle row comparison keys by processing subkeys
    if (skey->sk_flags & SK_ROW_HEADER)
    {
        ScanKey subkey = (ScanKey) DatumGetPointer(skey->sk_argument);

        for (;;)
        {
            addflags = indoption[subkey->sk_attno - 1] << SK_BT_INDOPTION_SHIFT;
            if ((addflags & SK_BT_DESC) && !(subkey->sk_flags & SK_BT_DESC))
                subkey->sk_strategy = BTCommuteStrategyNumber(subkey->sk_strategy);
            subkey->sk_flags |= addflags;

            if (subkey->sk_flags & SK_ROW_END)
                break;
            subkey++;
        }
    }

    return true;
}
```