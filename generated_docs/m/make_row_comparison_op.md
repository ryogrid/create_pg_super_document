# make_row_comparison_op

## Location
src/backend/parser/parse_expr.c: 2816 - 3017

## Overview
Transforms a "row compare-op row" construct by analyzing operator semantics and creating appropriate comparison expressions for multi-column row comparisons.

## Definition
```c
static Node *make_row_comparison_op(ParseState *pstate, List *opname, List *largs, List *rargs, int location)
```

## Detailed Description
The `make_row_comparison_op` function handles row comparison operations where two row expressions are compared using operators like =, <>, <, <=, >, or >=. It takes lists of already-transformed expressions from both sides of the comparison and determines the appropriate comparison semantics. The function first validates that both row expressions have equal length and creates pairwise operator expressions using `make_op`. For equality (=) and inequality (<>) operations, it combines the pairwise operators with AND or OR respectively. For ordering operators (<, <=, >, >=), it analyzes btree operator families to determine the correct interpretation and creates a RowCompareExpr node. The function ensures all operators return boolean values and validates that operators have consistent btree semantics across all column pairs.

## Parameters / Member Variables
- `pstate`: ParseState pointer for parsing context, may be NULL for special cases
- `opname`: List containing the operator name to be applied 
- `largs`: List of already-transformed expressions from the left side of the comparison
- `rargs`: List of already-transformed expressions from the right side of the comparison
- `location`: Source location for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - [make_op](make_op.md)
  - castNode
  - [expression_returns_set](../e/expression_returns_set.md)
  - [get_op_btree_interpretation](../g/get_op_btree_interpretation.md)
  - [bms_add_member](../b/bms_add_member.md)
  - [bms_int_members](../b/bms_int_members.md)
  - [bms_next_member](../b/bms_next_member.md)
  - [makeBoolExpr](makeBoolExpr.md)
  - lappend_oid
  - makeNode
- Called from (representative examples):
  - [transformAExprOp](../t/transformAExprOp.md)
  - transformAExprIn
  - transformSubLink

## Notes and Other Information
- Returns different node types based on the operation: single OpExpr for single columns, BoolExpr (AND/OR) for equality/inequality, or RowCompareExpr for ordering comparisons
- Validates that row expressions have equal length and non-zero length
- Requires operators to return boolean type directly, not via coercion
- Analyzes btree operator families to determine consistent comparison semantics
- For ambiguous operator interpretations, arbitrarily selects the lowest strategy number
- Handles operator coercions that may be inserted by make_op by reconstructing argument lists
- The function guarantees that the output always returns boolean type