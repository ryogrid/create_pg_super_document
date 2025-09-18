# make_andclause

## Location
src/backend/nodes/makefuncs.c: 701 - 716

## Overview
Creates an AND boolean expression node from a list of subclauses, used in PostgreSQL's query planning and execution system.

## Definition


## Detailed Description
The  function constructs a BoolExpr node representing an AND operation in PostgreSQL's expression tree. It takes a list of clause expressions and creates a single boolean expression that evaluates to true only when all subclauses are true. This function is fundamental to query optimization and execution, allowing the system to represent complex boolean logic in a structured tree format.

The function allocates a new BoolExpr node, sets its operation type to AND_EXPR, assigns the provided list of clauses as arguments, and sets the location to -1 (indicating no specific source location). The resulting expression can then be used throughout the query planning and execution pipeline.

## Parameters / Member Variables
- : A List of Expr pointers representing the individual clauses to be combined with AND logic

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create BoolExpr)
  - BoolExpr (expression node type)
  - AND_EXPR (boolean operation constant)
- Called from (representative examples):
  - [make_and_qual](make_and_qual.md)
  - [make_ands_explicit](make_ands_explicit.md)
  - [process_sublinks_mutator](../p/process_sublinks_mutator.md)
  - [pull_up_sublinks_qual_recurse](../p/pull_up_sublinks_qual_recurse.md)
  - [negate_clause](../n/negate_clause.md)
  - [find_duplicate_ors](../f/find_duplicate_ors.md)
  - [eval_const_expressions_mutator](../e/eval_const_expressions_mutator.md)
  - [make_sub_restrictinfos](make_sub_restrictinfos.md)

## Notes and Other Information
- The location field is set to -1, indicating that the clause doesn't correspond to a specific location in the original SQL text
- This function is part of PostgreSQL's node creation utilities in makefuncs.c
- The returned Expr pointer can be cast back to BoolExpr when needed
- Used extensively in query optimization phases like constant folding, sublink processing, and join condition handling