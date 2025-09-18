# _bt_advance_array_keys

## Location
src/backend/access/nbtree/nbtutils.c: 1789 - 2551

## Overview
Advances array elements using a tuple to determine new array positions, and serves as a wrapper around _bt_check_compare for requalification checks.

## Definition


## Detailed Description
This function is the core of PostgreSQL's B-tree array key advancement mechanism. It performs a complex process of advancing array scan keys based on tuple values, ensuring that the scan progresses correctly through the index while handling both required and non-required array keys.

The function operates by comparing each array element against the corresponding tuple attribute values and finding the appropriate next array position. It implements a "ratcheting" mechanism where required arrays can only advance forward (or backward in reverse scans) and never retreat. The function handles several scenarios:

1. **Exact matches**: When tuple values exactly match array elements
2. **Beyond-end advancement**: When tuple values exceed the closest array elements
3. **Rollover handling**: When advancement needs to carry to higher-order arrays
4. **Non-required arrays**: Special handling for optional array keys

The function also performs requalification by calling _bt_check_compare with the newly advanced keys to determine if the original tuple still satisfies the updated scan criteria. It can recursively call itself for "second pass" handling of required inequality scan keys that weren't initially detected.

## Parameters / Member Variables
- : Index scan descriptor containing array key state and scan information  
- : Page-level scan state for tracking page boundaries and scan direction
- : The index tuple that triggered array advancement
- : Number of attributes in the tuple
- : Tuple descriptor for attribute access
- : Index of the scan key that triggered the advancement
- : Whether the triggering scan key is required in the current scan direction

## Dependencies
- Functions called/Symbols referenced:
  - _bt_tuple_before_array_skeys
  - _bt_verify_keys_with_arraykeys  
  - _bt_binsrch_array_skey
  - _bt_compare_array_skey
  - _bt_advance_array_keys_increment
  - _bt_check_compare
  - _bt_rewind_nonrequired_arrays
  - _bt_parallel_primscan_schedule
  - index_getattr
  - BTreeTupleGetNAtts
- Called from (representative examples):
  - _bt_checkkeys
  - _bt_check_compare
  - _bt_advance_array_keys (recursive)

## Notes and Other Information
- Implements a complex state machine for array advancement with multiple phases
- Handles truncated attributes in high key tuples by setting scanBehind flag
- Supports both forward and backward scan directions with direction-specific logic
- Can trigger new primitive index scans when arrays become exhausted or when optimizations indicate page skipping opportunities
- Uses binary search for finding closest matching array elements
- Maintains strict ordering guarantees: arrays never advance beyond what's safe based on current tuple information
- Includes extensive optimization logic for handling opposite-direction inequality keys and NULL value boundaries
- Return value indicates whether the triggering tuple satisfies the newly advanced array keys