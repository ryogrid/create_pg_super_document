# transformWindowDefinitions

## Location
src/backend/parser/parse_clause.c: 2765 - 2984

## Overview
Transforms window definitions (WindowDef nodes) into WindowClause nodes, handling window references, partition/order clauses, and frame specifications according to SQL standard rules.

## Definition


## Detailed Description
This function processes WINDOW clause definitions and inline window specifications from SQL queries. For each WindowDef in the input list, it creates a corresponding WindowClause node with transformed partition and order clauses. The function handles complex window inheritance rules from SQL:2008 standard: referenced windows copy partition clauses (which cannot be overridden), may copy order clauses (only if the referenced window has none), and cannot copy frame clauses (referenced windows with frame clauses cause errors). It validates window name uniqueness, resolves window references, transforms PARTITION BY clauses using transformGroupClause, and transforms ORDER BY clauses using transformSortClause. Special handling is provided for RANGE frame mode with offsets (requires exactly one ORDER BY column) and GROUPS frame mode (requires an ORDER BY clause). Frame offset expressions are processed through transformFrameOffset.

## Parameters / Member Variables
- : ParseState containing parsing context and state information
- : List of WindowDef nodes to be transformed from the SQL WINDOW clause
- : Reference to TargetEntry list where partition/order expressions are added as resjunk

## Dependencies
- Functions called/Symbols referenced:
  - [findWindowClause](../f/findWindowClause.md)
  - [transformSortClause](transformSortClause.md)
  - [transformGroupClause](transformGroupClause.md)
  - [transformFrameOffset](transformFrameOffset.md)
  - copyObject
  - makeNode
  - [get_sortgroupclause_expr](../g/get_sortgroupclause_expr.md)
  - [get_ordering_op_properties](../g/get_ordering_op_properties.md)
  - [exprCollation](../e/exprCollation.md)
  - linitial_node
  - [WindowDef](../W/WindowDef.md), WindowClause, SortGroupClause (struct types)
  - EXPR_KIND_WINDOW_ORDER, EXPR_KIND_WINDOW_PARTITION (enum values)
  - FRAMEOPTION_DEFAULTS, FRAMEOPTION_RANGE, FRAMEOPTION_GROUPS, FRAMEOPTION_START_OFFSET, FRAMEOPTION_END_OFFSET (frame option constants)
  - BTLessStrategyNumber (B-tree strategy constant)
- Called from (representative examples):
  - [transformSelectStmt](transformSelectStmt.md)
  - [transformPLAssignStmt](transformPLAssignStmt.md)

## Notes and Other Information
- This is a public function declared in parse_clause.h
- Implements SQL:2008 window clause inheritance rules with their complex and somewhat bizarre semantics
- Always forces SQL99 rules for partition and order clause interpretation regardless of server settings
- Enforces strict validation for window references and prevents circular references
- RANGE mode with offsets requires exactly one ORDER BY column and determines sort operator properties
- GROUPS mode mandates the presence of an ORDER BY clause
- Window reference numbers (winref) are assigned sequentially for query execution
- Frame clause inheritance is prohibited by SQL standard, leading to specific error messages
- Part of PostgreSQL's comprehensive window function support implementing SQL standard windowing