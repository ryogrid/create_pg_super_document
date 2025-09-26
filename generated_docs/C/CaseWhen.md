# CaseWhen

## Location
src/include/nodes/primnodes.h: 1322 - 1328

## Overview
CaseWhen represents one arm of a CASE expression in PostgreSQL's expression tree, containing a condition and its corresponding result value.

## Definition


## Detailed Description
CaseWhen is a node type that represents a single WHEN clause within a CASE expression. Each CaseWhen node contains a condition expression and a result expression. When the condition evaluates to true, the corresponding result is returned. This structure is part of PostgreSQL's expression tree representation and is used during parsing, planning, and execution of SQL CASE statements.

The structure inherits from Expr, making it a proper expression node that can be integrated into the broader expression tree. The location field helps with error reporting by tracking where in the original SQL text this WHEN clause appeared.

## Parameters / Member Variables
- : Base expression node structure (inherited from Expr)
- : The condition expression that is evaluated to determine if this WHEN arm should be selected
- : The expression whose value is returned if the condition is true
- : Parse location in the original SQL text for error reporting, or -1 if location is unknown

## Dependencies
- Functions called/Symbols referenced:
  - ParseLoc (for location tracking)
  - Expr (base expression structure)

- Called from (representative examples):
  - ExecInitExprRec (expression initialization during execution)
  - transformCaseExpr (parser transformation of CASE expressions)
  - eval_const_expressions_mutator (constant folding optimization)
  - get_rule_expr (rule decompilation for pg_dump and view definitions)
  - assign_collations_walker (collation assignment during parsing)

## Notes and Other Information
- CaseWhen nodes are typically created during the parsing phase when transforming SQL CASE expressions
- Multiple CaseWhen nodes are collected in a list to represent all WHEN arms of a single CASE expression
- The expression tree walker and mutator functions handle CaseWhen nodes specially to traverse both the condition and result expressions
- This structure is used in conjunction with CaseExpr which contains the overall CASE expression including the list of CaseWhen arms and optional ELSE clause