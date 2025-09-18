# _bt_mark_scankey_required

## Location
[src/backend/access/nbtree/nbtutils.c:3438-3507](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L3438-L3507)

## Overview
Marks a scankey as "required to continue the scan" by setting appropriate directional flags based on the operator strategy, handling both simple and row comparison scankeys.

## Definition
static void _bt_mark_scankey_required(ScanKey skey)

## Detailed Description
This function determines which scan directions require a particular scan key and marks it accordingly. The marking affects scan termination decisions during B-tree traversal:

1. **Strategy-based marking**: Different comparison operators require the key for different scan directions:
   - Less/LessEqual: Required for forward scans only (SK_BT_REQFWD)
   - Equal: Required for both forward and backward scans (SK_BT_REQFWD | SK_BT_REQBKWD)  
   - GreaterEqual/Greater: Required for backward scans only (SK_BT_REQBKWD)

2. **Row scankey handling**: For SK_ROW_HEADER scankeys, it also marks the first subsidiary scankey with the same flags, since row comparisons depend on their leading column.

3. **Scan optimization**: These flags help the scan engine determine when it can terminate early - if a required key's condition is no longer satisfiable in a particular direction, the scan can stop.

The markings persist across rescans within a query since the requirements don't change.

## Parameters / Member Variables
- : ScanKey - The scan key to mark as required (modified in-place)

## Dependencies
- Functions called/Symbols referenced:
  - Strategy constants (BTLessStrategyNumber, BTLessEqualStrategyNumber, BTEqualStrategyNumber, BTGreaterEqualStrategyNumber, BTGreaterStrategyNumber)
  - Required direction flags (SK_BT_REQFWD, SK_BT_REQBKWD)
  - Row scankey flags (SK_ROW_HEADER, SK_ROW_MEMBER)
  - [DatumGetPointer](../D/DatumGetPointer.md) (extracting subkey array from row header)
  - elog (error reporting for unrecognized strategies)

- Called from (representative examples):
  - [_bt_preprocess_keys](_bt_preprocess_keys.md) (multiple calls during scan key preprocessing to mark keys as required)

## Notes and Other Information
- This is a void function that modifies scankey flags in-place
- Scribbles on data structures belonging to the index AM's caller, not private copies
- Safe for rescans since markings would be identical each time
- Only marks the first subsidiary scankey in row comparisons, as subsequent keys are for lower-order columns and cannot be required after the first non-equality condition
- Part of PostgreSQL's scan optimization system that enables early termination
- The required flags are used by scan positioning and advancement logic to determine when scans can terminate