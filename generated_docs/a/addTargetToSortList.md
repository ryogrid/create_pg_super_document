# addTargetToSortList

## Location
src/backend/parser/parse_clause.c: 3393 - 3535

## Overview
Adds a target list entry to a SortGroupClause list if it's not already present, with specified sort ordering information.

## Definition


## Detailed Description
This function is a core component of PostgreSQL's query parsing infrastructure for handling ORDER BY clauses. It ensures that each target list entry appears only once in the sort list while handling various sorting specifications like ASC/DESC, custom operators (USING clause), and NULL ordering preferences.

The function performs type coercion for UNKNOWN literals to TEXT, determines appropriate sort and equality operators based on the sort direction, and handles error reporting with proper parse position context. It creates a SortGroupClause node with all necessary sorting metadata including sort operators, equality operators, hashability information, and null ordering preferences.

## Parameters / Member Variables
- : Parse state containing context information for query parsing
- : Target entry to be added to the sort list
- : Current list of SortGroupClause nodes
- : Complete target list for the query
- : Sort specification containing direction, operator, and null ordering

## Dependencies
- Functions called/Symbols referenced:
  - coerce_type
  - setup_parser_errposition_callback
  - get_sort_group_operators
  - compatible_oper_opid
  - get_equality_op_for_ordering_op
  - op_hashjoinable
  - targetIsInSortList
  - assignSortGroupRef
- Called from (representative examples):
  - transformSortClause
  - transformAggregateCall

## Notes and Other Information
- Handles type coercion for UNKNOWN literals to TEXT type automatically
- Supports ASC, DESC, and custom USING operators for sorting
- Prevents duplicate entries in the sort list through targetIsInSortList check
- Provides comprehensive error reporting with parse position context
- Manages NULL ordering preferences (NULLS FIRST/LAST) with sensible defaults
- Determines operator hashability for potential hash-based sorting optimizations