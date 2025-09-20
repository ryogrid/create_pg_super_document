# get_rel_all_updated_cols

## Location
[src/backend/optimizer/util/inherit.c:656-709](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/inherit.c#L656-L709)

## Overview
Returns the complete set of columns for a given relation that are updated by the current UPDATE query, including both directly updated columns and dependent generated columns.

## Definition

```c
union(updatedCols, extraUpdatedCols);
```
## Detailed Description
This function determines all columns that will be affected by an UPDATE operation on a specified relation. It performs several key operations:

1. **Initial Column Set**: Retrieves the updatedCols from the RTEPermissionInfo of the query's result relation, which contains the directly updated columns.

2. **Column Mapping**: If the requested relation differs from the result relation (e.g., in inheritance hierarchies), it translates the column numbers using translate_col_privs_multilevel to account for differences in column ordering between parent and child relations.

3. **Generated Column Dependencies**: Identifies any generated columns that depend on the updated columns using get_dependent_generated_columns, since generated columns must be recalculated when their dependencies change.

4. **Union Result**: Combines the directly updated columns with dependent generated columns to return the complete set of affected columns.

This function is essential for UPDATE planning in inheritance hierarchies and ensures that all affected columns, including generated ones, are properly identified and handled.

## Parameters / Member Variables
- : PlannerInfo structure containing global planner state and query information
- : RelOptInfo for the relation whose updated columns are being requested

## Dependencies
- Functions called/Symbols referenced:
  - planner_rt_fetch
  - [getRTEPermissionInfo](getRTEPermissionInfo.md)
  - [find_base_rel](../f/find_base_rel.md)
  - [translate_col_privs_multilevel](../t/translate_col_privs_multilevel.md)
  - [get_dependent_generated_columns](get_dependent_generated_columns.md)
  - [bms_union](../b/bms_union.md)
  - IS_SIMPLE_REL, IS_OTHER_REL macros
- Called from (representative examples):
  - Referenced in header file INHERIT_H

## Notes and Other Information
- Only operates on UPDATE commands (asserts commandType == CMD_UPDATE)
- Requires the input relation to be a simple relation (asserts IS_SIMPLE_REL)
- For inheritance hierarchies, performs multilevel column privilege translation
- Handles generated columns by including any that depend on directly updated columns
- Returns a Bitmapset representing column attribute numbers that will be modified
- Essential for proper UPDATE planning in partitioned and inherited tables where column numbering may differ between relations