# BTArrayKeyInfo

## Location
[src/include/access/nbtree.h:1032-1038](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/nbtree.h#L1032-L1038)

## Overview
BTArrayKeyInfo manages array scan keys in B-tree index scans, tracking the current position and elements within equality-type SK_SEARCHARRAY scan keys.

## Definition

```c
typedef struct BTArrayKeyInfo
{
	int			scan_key;		/* index of associated key in keyData */
	int			cur_elem;		/* index of current element in elem_values */
	int			num_elems;		/* number of elems in current array value */
	Datum	   *elem_values;	/* array of num_elems Datums */
} BTArrayKeyInfo;
```
## Detailed Description
This structure is used to manage array-based scan keys in B-tree index operations. When a scan involves SK_SEARCHARRAY type keys (which allow searching for any value in an array), this structure tracks the current state of iteration through the array elements. It enables efficient scanning by maintaining the current position within the array and providing direct access to the array elements as Datum values.

## Parameters / Member Variables
- `scan_key`: Integer index of the associated scan key in the keyData array
- `cur_elem`: Integer index pointing to the current element being processed in elem_values
- `num_elems`: Integer count of the total number of elements in the current array value
- `*elem_values`: Pointer to an array of Datum values containing the actual array elements
## Dependencies
- Functions called/Symbols referenced:
  - Datum
- Called from (representative examples):
  - [_bt_parallel_seize](../b/_bt_parallel_seize.md)
  - [_bt_parallel_primscan_schedule](../b/_bt_parallel_primscan_schedule.md)
  - [_bt_preprocess_array_keys](../b/_bt_preprocess_array_keys.md)
  - [_bt_preprocess_array_keys_final](../b/_bt_preprocess_array_keys_final.md)
  - [_bt_compare_array_scankey_args](../b/_bt_compare_array_scankey_args.md)
  - [_bt_binsrch_array_skey](../b/_bt_binsrch_array_skey.md)
  - [_bt_start_array_keys](../b/_bt_start_array_keys.md)
  - [_bt_advance_array_keys_increment](../b/_bt_advance_array_keys_increment.md)
  - [_bt_rewind_nonrequired_arrays](../b/_bt_rewind_nonrequired_arrays.md)
  - [_bt_advance_array_keys](../b/_bt_advance_array_keys.md)
  - [_bt_preprocess_keys](../b/_bt_preprocess_keys.md)
  - [_bt_verify_arrays_bt_first](../b/_bt_verify_arrays_bt_first.md)
  - [_bt_verify_keys_with_arraykeys](../b/_bt_verify_keys_with_arraykeys.md)
  - [_bt_compare_scankey_args](../b/_bt_compare_scankey_args.md)
  - [BTScanOpaqueData](BTScanOpaqueData.md)

## Notes and Other Information
- Essential for handling array-based search conditions in B-tree indexes
- Enables efficient iteration through multiple values in a single scan operation
- Used in parallel scanning operations and scan key preprocessing
- Part of the broader scan key management infrastructure in PostgreSQL's B-tree implementation
- Supports complex query patterns involving IN clauses and array operations