# get_tle_by_resno

## Location
src/backend/parser/parse_relation.c: 3439 - 3458

## Overview
Searches a target list to find and return the TargetEntry with a matching result number (resno).

## Definition


## Detailed Description
This utility function performs a linear search through a target list to locate a TargetEntry with the specified result number. Unlike simple list indexing, this function is necessary because target lists are not always sorted by resno. The function iterates through each TargetEntry in the list and compares its resno field with the requested value.

The function is widely used throughout PostgreSQL's query processing components, particularly in:
- Query planning and optimization
- Target list manipulation and analysis
- View rewriting and variable substitution
- EXPLAIN output generation
- Rule utilities and variable resolution

## Parameters / Member Variables
- : List of TargetEntry structures to search through
- : The result number (attribute number) to search for

## Dependencies
- Functions called/Symbols referenced:
  - RowMarkClause (referenced but not called within this function)
  - Standard PostgreSQL list iteration macros (foreach, lfirst)
- Called from (representative examples):
  - show_grouping_set_keys (EXPLAIN functionality)
  - show_sort_group_keys (EXPLAIN functionality)
  - create_unique_plan (query planning)
  - prepare_sort_from_pathkeys (query planning)
  - markTargetListOrigin (target list analysis)
  - expandRecordVariable (variable expansion)
  - rewriteTargetView (view rewriting)
  - get_variable (rule utilities)
  - resolve_special_varno (variable resolution)

## Notes and Other Information
- Returns NULL if no TargetEntry with the specified resno is found in the list
- The linear search approach is necessary because target lists may not maintain sorted order by resno
- This is a fundamental utility function used extensively across PostgreSQL's query processing subsystems
- Performance consideration: For large target lists accessed frequently, consider using indexed access if the list is known to be sorted