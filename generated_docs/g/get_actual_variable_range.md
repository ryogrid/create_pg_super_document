# get_actual_variable_range

## Location
src/backend/utils/adt/selfuncs.c: 6153 - 6332

## Overview
Attempts to identify the current actual minimum and/or maximum values of a specified variable by searching for a suitable B-tree index and fetching its low and/or high values from the actual table data.

## Definition
```c
static bool get_actual_variable_range(PlannerInfo *root, VariableStatData *vardata,
                                     Oid sortop, Oid collation,
                                     Datum *min, Datum *max)
```

## Detailed Description
This function provides a mechanism to obtain real-time minimum and maximum values for a column by performing actual index scans rather than relying solely on stored statistics. It searches through available B-tree indexes on the relation to find one that matches the specified variable, sort operator, and collation. Once a suitable index is found, it performs index scans in both directions (forward for minimum, backward for maximum) to retrieve the actual extreme values. The function handles various edge cases including partitioned tables, partial indexes, and hypothetical indexes, ensuring it only uses indexes that provide complete coverage of the relation.

## Parameters / Member Variables
- `root`: PlannerInfo containing query planning context and relation information
- `vardata`: VariableStatData structure containing information about the variable being analyzed
- `sortop`: OID of the "<" comparison operator to use for ordering
- `collation`: Required collation for the comparison operations
- `min`: Pointer to store the minimum value found (can be NULL if not needed)
- `max`: Pointer to store the maximum value found (can be NULL if not needed)

## Dependencies
- Functions called/Symbols referenced:
  - VariableStatData
  - RTE_RELATION
  - [IndexOptInfo](../I/IndexOptInfo.md)
  - [match_index_to_operand](../m/match_index_to_operand.md)
  - [get_op_opfamily_strategy](get_op_opfamily_strategy.md)
  - AllocSetContextCreate
  - [index_open](../i/index_open.md)
  - [table_slot_create](../t/table_slot_create.md)
  - [get_typlenbyval](get_typlenbyval.md)
  - [ScanKeyEntryInitialize](../S/ScanKeyEntryInitialize.md)
  - get_actual_variable_endpoint
  - [ExecDropSingleTupleTableSlot](../E/ExecDropSingleTupleTableSlot.md)
  - [index_close](../i/index_close.md)
- Called from (representative examples):
  - ineq_histogram_selectivity
  - get_variable_range

## Notes and Other Information
This function is particularly useful when stored statistics are outdated or when precise range information is critical for query optimization. It creates a temporary memory context to ensure proper cleanup of resources used during index scanning. The function only considers B-tree indexes since they maintain ordered data, and it skips partial indexes to ensure complete relation coverage. When both minimum and maximum values are requested, it performs two separate index scans in opposite directions. The function respects existing locks and uses NoLock when opening relations, assuming appropriate locks are already held by the calling context.