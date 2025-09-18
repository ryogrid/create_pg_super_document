# PgBenchExprLink

## Location
src/bin/pgbench/pgbench.h: 107 - 107

## Overview
PgBenchExprLink is a linked list node structure that chains together PgBenchExpr nodes, primarily used for representing function argument lists in pgbench expressions.

## Definition


## Detailed Description
PgBenchExprLink implements a singly-linked list data structure to organize sequences of PgBenchExpr nodes. This is essential for representing function calls with multiple arguments in pgbench expressions. Each link contains a pointer to an expression and a pointer to the next link in the chain. The structure enables recursive expression evaluation where functions can have arbitrary numbers of arguments, each of which can be complex expressions themselves.

## Parameters / Member Variables
- : Pointer to a PgBenchExpr node that represents one expression in the sequence
- : Pointer to the next PgBenchExprLink in the linked list, or NULL if this is the last node

## Dependencies
- Functions called/Symbols referenced:
  - [PgBenchExpr](PgBenchExpr.md) (struct)
- Called from (representative examples):
  - evalLazyFunc
  - evalStandardFunc
  - evalFunc
  - [PgBenchExpr](PgBenchExpr.md) (in function.args member)
  - [PgBenchExprList](PgBenchExprList.md) (in head/tail members)

## Notes and Other Information
- Forward declared at line 107 in pgbench.h, with full definition at lines 129-133
- Implements a standard singly-linked list pattern for chaining expressions
- Used extensively in function argument processing and evaluation
- Part of the expression evaluation infrastructure that allows pgbench to handle complex nested expressions
- Memory management for these nodes follows the overall pgbench expression lifecycle