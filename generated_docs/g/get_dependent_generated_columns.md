# get_dependent_generated_columns

## Location
src/backend/optimizer/util/plancat.c: 2371 - 2418

## Overview
Identifies stored generated columns that depend on any of the specified target columns, returning a bitmapset of dependent column numbers for use in query planning and execution.

## Definition


## Detailed Description
This function analyzes the dependency relationships between columns in a relation to find stored generated columns that depend on any column in the provided target set. It examines each stored generated column's expression to determine which base columns it references, then checks if any of those referenced columns overlap with the target columns.

The function works by:
1. Opening the relation and accessing its tuple descriptor
2. Iterating through all default value definitions in the constraint structure
3. For each stored generated column, parsing its expression and extracting referenced attributes
4. Checking if the referenced attributes overlap with the target columns
5. Adding dependent generated columns to the result bitmapset

This is particularly important for UPDATE operations where modifying a base column requires updating dependent generated columns, and for determining which columns need to be included in result sets.

## Parameters / Member Variables
- : PlannerInfo structure containing planning context and information
- : Range table index identifying the relation to analyze
- : Bitmapset of column numbers (offset by FirstLowInvalidHeapAttributeNumber) to check dependencies against

## Dependencies
- Functions called/Symbols referenced:
  - planner_rt_fetch
  - table_open
  - table_close
  - RelationGetDescr
  - TupleDescAttr
  - [stringToNode](../s/stringToNode.md)
  - [pull_varattnos](../p/pull_varattnos.md)
  - [bms_overlap](../b/bms_overlap.md)
  - [bms_add_member](../b/bms_add_member.md)
  - [TupleConstr](../T/TupleConstr.md)
  - [AttrDefault](../A/AttrDefault.md)
  - FirstLowInvalidHeapAttributeNumber
- Called from (representative examples):
  - [get_rel_all_updated_cols](get_rel_all_updated_cols.md)

## Notes and Other Information
- Column numbers in both input and output bitmapsets are offset by FirstLowInvalidHeapAttributeNumber
- The function assumes adequate locking has already been acquired for the relation
- Used primarily in inheritance planning and UPDATE operations to ensure all dependent generated columns are properly handled
- The dependency analysis is performed by parsing the stored expression text and extracting variable attribute numbers
- Only stored generated columns are considered; virtual generated columns are not included in the analysis