# addTargetToGroupList

## Location
src/backend/parser/parse_clause.c: 3536 - 3590

## Overview
Adds a target list entry to a SortGroupClause list for grouping operations if not already present, using default sort/group semantics.

## Definition


## Detailed Description
This static function is similar to addTargetToSortList but specifically designed for GROUP BY clause processing. It differs in that it only requires a grouping (equality) operator and considers a target entry "already in the list" if it appears with any sorting semantics. The function ensures that each grouping expression appears only once in the group list.

The function performs the same type coercion for UNKNOWN literals as addTargetToSortList and uses default sort/group semantics. It creates SortGroupClause nodes with equality operators required for grouping, optional sort operators, and hashability information for optimization purposes.

## Parameters / Member Variables
- : Parse state containing context information for query parsing
- : Target entry to be added to the group list
- : Current list of SortGroupClause nodes for grouping
- : Complete target list for the query  
- : Parse location for error reporting (cannot rely on tle->expr location)

## Dependencies
- Functions called/Symbols referenced:
  - [coerce_type](../c/coerce_type.md)
  - [targetIsInSortList](../t/targetIsInSortList.md)
  - [setup_parser_errposition_callback](../s/setup_parser_errposition_callback.md)
  - [get_sort_group_operators](../g/get_sort_group_operators.md)
  - [assignSortGroupRef](assignSortGroupRef.md)
- Called from (representative examples):
  - [transformGroupClauseExpr](../t/transformGroupClauseExpr.md)
  - [transformDistinctClause](../t/transformDistinctClause.md)
  - [transformDistinctOnClause](../t/transformDistinctOnClause.md)

## Notes and Other Information
- Static function internal to parse_clause.c for GROUP BY processing
- More permissive than addTargetToSortList - allows cases where only equality operator exists
- Uses InvalidOid when checking for duplicates with targetIsInSortList
- Sets nulls_first to false by default for grouping operations
- Location parameter is crucial since tle->expr location might point to SELECT item rather than GROUP BY item
- Handles UNKNOWN literal type coercion automatically like addTargetToSortList