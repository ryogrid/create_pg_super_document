# BTArrayKeyInfo

## Location
src/include/access/nbtree.h: 1032 - 1038

## Overview
BTArrayKeyInfo manages array scan keys in B-tree index scans, tracking the current position and elements within equality-type SK_SEARCHARRAY scan keys.

## Definition


## Detailed Description
This structure is used to manage array-based scan keys in B-tree index operations. When a scan involves SK_SEARCHARRAY type keys (which allow searching for any value in an array), this structure tracks the current state of iteration through the array elements. It enables efficient scanning by maintaining the current position within the array and providing direct access to the array elements as Datum values.

## Parameters / Member Variables
- : Integer index of the associated scan key in the keyData array
- : Integer index pointing to the current element being processed in elem_values
- : Integer count of the total number of elements in the current array value
- : Pointer to an array of Datum values containing the actual array elements

## Dependencies
- Functions called/Symbols referenced:
  - Datum
- Called from (representative examples):
  - _bt_parallel_seize
  - _bt_parallel_primscan_schedule
  - _bt_preprocess_array_keys
  - _bt_preprocess_array_keys_final
  - _bt_compare_array_scankey_args
  - _bt_binsrch_array_skey
  - _bt_start_array_keys
  - _bt_advance_array_keys_increment
  - _bt_rewind_nonrequired_arrays
  - _bt_advance_array_keys
  - _bt_preprocess_keys
  - _bt_verify_arrays_bt_first
  - _bt_verify_keys_with_arraykeys
  - _bt_compare_scankey_args
  - BTScanOpaqueData

## Notes and Other Information
- Essential for handling array-based search conditions in B-tree indexes
- Enables efficient iteration through multiple values in a single scan operation
- Used in parallel scanning operations and scan key preprocessing
- Part of the broader scan key management infrastructure in PostgreSQL's B-tree implementation
- Supports complex query patterns involving IN clauses and array operations