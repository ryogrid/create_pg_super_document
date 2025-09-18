# free_attstatsslot

## Location
src/backend/utils/cache/lsyscache.c: 3344 - 3365

## Overview
Frees memory allocated by get_attstatsslot for an AttStatsSlot structure.

## Definition


## Detailed Description
The free_attstatsslot function is responsible for properly deallocating memory that was allocated by the get_attstatsslot function. It handles the cleanup of various components within an AttStatsSlot structure, including the values array, detoasted array objects, and numbers array. The function follows PostgreSQL's memory management conventions using pfree() for memory allocated with palloc().

## Parameters / Member Variables
- : Pointer to an AttStatsSlot structure containing statistical information about a table column that needs to be freed

## Dependencies
- Functions called/Symbols referenced:
  - pfree (memory deallocation function)
  - AttStatsSlot (structure type)
- Called from (representative examples):
  - ExecHashBuildSkewHash
  - tsquerysel
  - scalararraysel_containment
  - calc_arraycontsel
  - calc_multirangesel
  - calc_hist_selectivity
  - networksel
  - var_eq_const
  - mcv_selectivity
  - histogram_selectivity
  - eqjoinsel
  - get_variable_range
  - btcostestimate

## Notes and Other Information
- The values[] array was separately allocated by deconstruct_array and requires explicit freeing
- The numbers[] array points into numbers_arr and should not be freed directly
- Handles detoasted array objects (values_arr and numbers_arr) that may have been created during statistics processing
- This function is critical for preventing memory leaks when working with column statistics in PostgreSQL
- Used extensively throughout the selectivity estimation and cost calculation subsystems