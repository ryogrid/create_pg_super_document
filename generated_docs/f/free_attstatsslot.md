# free_attstatsslot

## Location
[src/backend/utils/cache/lsyscache.c:3344-3365](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L3344-L3365)

## Overview
Frees memory allocated by get_attstatsslot for an AttStatsSlot structure.

## Definition

```c
struct_array */
	if (sslot->values)
		pfree(sslot->values);
```
## Detailed Description
The free_attstatsslot function is responsible for properly deallocating memory that was allocated by the get_attstatsslot function. It handles the cleanup of various components within an AttStatsSlot structure, including the values array, detoasted array objects, and numbers array. The function follows PostgreSQL's memory management conventions using pfree() for memory allocated with palloc().

## Parameters / Member Variables
- : Pointer to an AttStatsSlot structure containing statistical information about a table column that needs to be freed

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md) (memory deallocation function)
  - [AttStatsSlot](../A/AttStatsSlot.md) (structure type)
- Called from (representative examples):
  - [ExecHashBuildSkewHash](../E/ExecHashBuildSkewHash.md)
  - [tsquerysel](../t/tsquerysel.md)
  - [scalararraysel_containment](../s/scalararraysel_containment.md)
  - [calc_arraycontsel](../c/calc_arraycontsel.md)
  - [calc_multirangesel](../c/calc_multirangesel.md)
  - [calc_hist_selectivity](../c/calc_hist_selectivity.md)
  - [networksel](../n/networksel.md)
  - [var_eq_const](../v/var_eq_const.md)
  - [mcv_selectivity](../m/mcv_selectivity.md)
  - [histogram_selectivity](../h/histogram_selectivity.md)
  - [eqjoinsel](../e/eqjoinsel.md)
  - [get_variable_range](../g/get_variable_range.md)
  - [btcostestimate](../b/btcostestimate.md)

## Notes and Other Information
- The values[] array was separately allocated by deconstruct_array and requires explicit freeing
- The numbers[] array points into numbers_arr and should not be freed directly
- Handles detoasted array objects (values_arr and numbers_arr) that may have been created during statistics processing
- This function is critical for preventing memory leaks when working with column statistics in PostgreSQL
- Used extensively throughout the selectivity estimation and cost calculation subsystems