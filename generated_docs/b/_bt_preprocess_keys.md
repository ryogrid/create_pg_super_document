# _bt_preprocess_keys

## Location
[src/backend/access/nbtree/nbtutils.c:2552-3005](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L2552-L3005)

## Overview
Preprocesses scan keys by transforming, sorting, eliminating redundancies, detecting contradictions, and marking keys as required for continuing the scan.

## Definition


## Detailed Description
This function is a comprehensive scan key preprocessing routine that transforms the input scan keys from scan->keyData[] into processed output keys in so->keyData[]. It performs multiple critical operations:

1. **Key Transformation**: Applies index options (DESC, NULLS_FIRST) and commutes strategy numbers for DESC columns
2. **Redundancy Elimination**: Keeps only the tightest bounds (one = key, or one >/>= and one </<= key per attribute)  
3. **Contradiction Detection**: Identifies impossible conditions like "x = 1 AND x > 2" and sets qual_ok = false
4. **Required Key Marking**: Marks keys with SK_BT_REQFWD/SK_BT_REQBKWD flags based on scan continuation requirements
5. **Array Key Processing**: Handles SK_SEARCHARRAY keys through specialized array preprocessing

The function implements a sophisticated algorithm for determining which keys must be satisfied to continue scanning. Keys for leading attributes with equality conditions are marked as required in both directions. For the first non-equality attribute, < and <= keys are marked as forward-required while > and >= keys are marked as backward-required.

The preprocessing handles incomplete operator families gracefully - if cross-type operators are missing, redundant keys may not be eliminated, but the scan will still work correctly.

## Parameters / Member Variables
- : Index scan descriptor containing input keys in keyData[] and receiving processed keys in opaque->keyData[]

## Dependencies
- Functions called/Symbols referenced:
  - [_bt_verify_keys_with_arraykeys](_bt_verify_keys_with_arraykeys.md)
  - [_bt_preprocess_array_keys](_bt_preprocess_array_keys.md)
  - [_bt_fix_scankey_strategy](_bt_fix_scankey_strategy.md)
  - [_bt_mark_scankey_required](_bt_mark_scankey_required.md)
  - [_bt_compare_scankey_args](_bt_compare_scankey_args.md)
  - [_bt_preprocess_array_keys_final](_bt_preprocess_array_keys_final.md)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
- Called from (representative examples):
  - [_bt_first](_bt_first.md)

## Notes and Other Information
- Only performs preprocessing once per btrescan - subsequent calls are no-ops
- Expects input keys to be sorted by attribute (verified with assertions)
- Handles row comparison keys by passing them through without modification
- Creates temporary keyDataMap for remapping orderProc arrays when array keys are present
- Sets so->qual_ok = false and returns early when contradictory or unmatchable conditions are detected
- For single key optimization, bypasses most processing but still applies indoption transformations
- Maintains array key ordering consistency required by _bt_advance_array_keys
- The numberOfEqualCols tracking is crucial for determining which subsequent keys can be marked as required
- Returns with so->numberOfKeys set to the number of processed output keys (may be less than input)