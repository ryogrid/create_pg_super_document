# _bt_setup_array_cmp

## Location
src/backend/access/nbtree/nbtutils.c: 712 - 788

## Overview
Sets up array comparison functions by configuring ORDER procedures for binary searches during index scans and for sorting arrays, handling both same-type and cross-type comparisons.

## Definition


## Detailed Description
This function configures the comparison procedures needed for array operations in B-tree scans. It handles two distinct scenarios:

1. **Same-Type Comparisons**: When the scan key element type matches the index column's opclass input type, it uses the cached comparison function from the index, setting both orderproc and sortprocp to point to the same function for efficiency.

2. **Cross-Type Comparisons**: When element types differ, it looks up appropriate comparison functions in the opfamily:
   - Sets up a cross-type ORDER proc for binary searches (using opclass input type vs. array element type)
   - Sets up a same-type ORDER proc for sorting the array (using element type vs. element type)

The function is called during preprocessing for all equality strategy scan keys, including non-array scalar equality keys that need to be treated as degenerate single-element arrays for consistency in _bt_advance_array_keys.

The ORDER procedures are essential for:
- Binary searches within sorted arrays during index scans
- Proper sorting and deduplication of array elements
- Maintaining consistency with index column ordering

## Parameters / Member Variables
- : The index scan descriptor containing scan context and relation information
- : The scan key for which to set up comparison procedures
- : The OID of the array element data type
- : Pointer to store the ORDER procedure for binary searches
- : Pointer to pointer for storing the same-type ORDER procedure for sorting (can be NULL for non-array keys)

## Dependencies
- Functions called/Symbols referenced:
  - BTScanOpaque
  - [index_getprocinfo](../i/index_getprocinfo.md)
  - [get_opfamily_proc](../g/get_opfamily_proc.md)
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md)
  - RegProcedureIsValid
  - RelationGetRelationName
  - BTEqualStrategyNumber
  - BTORDER_PROC
  - OidIsValid

- Called from (representative examples):
  - [_bt_preprocess_array_keys](_bt_preprocess_array_keys.md) (during array preprocessing)
  - [_bt_preprocess_array_keys_final](_bt_preprocess_array_keys_final.md) (for non-array equality keys)

## Notes and Other Information
- The function assumes the scan key uses BTEqualStrategyNumber strategy
- Requires a valid elemtype OID input
- For same-type comparisons, sortprocp is set to point to the same memory as orderproc for efficiency
- For cross-type comparisons, separate procedures are looked up and stored
- The cross-type ORDER proc uses opclass input type as left argument and array element type as right argument, matching the pattern used in binary searches
- Missing support functions result in ERROR conditions, typically indicating incomplete opfamily definitions
- All comparison function information is stored in the scan's array context memory
- The function handles both array scan keys (with sortprocp provided) and non-array scan keys (with sortprocp as NULL)