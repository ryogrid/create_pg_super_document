# _bt_compare_array_skey

## Location
[src/backend/access/nbtree/nbtutils.c:1131-1200](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L1131-L1200)

## Overview
Compares a tuple attribute value to a scan key/array element during binary searches of SK_SEARCHARRAY arrays in PostgreSQL's B-tree index operations.

## Definition


## Detailed Description
This function serves as a helper routine for binary searches within SK_SEARCHARRAY arrays in B-tree index operations. It provides a standardized comparison interface that handles NULL values according to the index's NULL ordering policy and applies proper collation rules for cross-type comparisons. 

The function returns comparison results in the standard format: negative values indicate tupdatum < arrdatum, zero indicates equality, and positive values indicate tupdatum > arrdatum. Unlike , this function takes the tuple argument first and the array/scankey argument second, which provides a more natural ordering for array search operations.

The function handles NULL value comparisons based on the SK_BT_NULLS_FIRST flag and properly inverts comparison results for DESC columns using the SK_BT_DESC flag.

## Parameters / Member Variables
- : FmgrInfo pointer to the ordering procedure function used for comparison
- : The datum value from the tuple being compared
- : Boolean indicating whether tupdatum is NULL
- : The datum value from the array element being compared against
- : ScanKey containing comparison metadata (strategy, flags, collation)

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetInt32](../D/DatumGetInt32.md)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md)
  - INVERT_COMPARE_RESULT
  - ScanKey (structure)
  - SK_ISNULL (flag)
  - SK_BT_NULLS_FIRST (flag)
  - SK_BT_DESC (flag)
- Called from (representative examples):
  - [_bt_binsrch_array_skey](_bt_binsrch_array_skey.md)
  - [_bt_tuple_before_array_skeys](_bt_tuple_before_array_skeys.md)
  - [_bt_advance_array_keys](_bt_advance_array_keys.md)

## Notes and Other Information
- The function assumes the scan key strategy is BTEqualStrategyNumber
- Cross-type comparisons are handled carefully by passing tupdatum as the left operand
- NULL handling follows B-tree index NULL ordering policies
- Unlike , this function uses natural argument ordering (tuple first, array second)
- The comparison result sign is flipped for DESC columns without the compensatory logic needed in 