# make_orclause

## Location
[src/backend/nodes/makefuncs.c:717-732](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/makefuncs.c#L717-L732)

## Overview
Creates an OR boolean expression node from a list of subclauses, used in PostgreSQL's query planning and execution system.

## Definition
```c
Expr *make_orclause(List *orclauses)
```

## Detailed Description
The `make_orclause` function constructs a BoolExpr node representing an OR operation in PostgreSQL's expression tree. It takes a list of clause expressions and creates a single boolean expression that evaluates to true when any of the subclauses is true. This function is essential for query optimization and execution, allowing the system to represent disjunctive boolean logic in a structured tree format.

Similar to `make_andclause`, this function allocates a new BoolExpr node, sets its operation type to OR_EXPR, assigns the provided list of clauses as arguments, and sets the location to -1. The resulting OR expression is widely used in query planning phases including bitmap index scan optimization, sublink processing, and constant expression evaluation.

## Parameters / Member Variables
- `orclauses`: A List of Expr pointers representing the individual clauses to be combined with OR logic

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create BoolExpr)
  - BoolExpr (expression node type)
  - OR_EXPR (boolean operation constant)
- Called from (representative examples):
  - [create_bitmap_subplan](../c/create_bitmap_subplan.md)
  - [create_tidscan_plan](../c/create_tidscan_plan.md)
  - [process_sublinks_mutator](../p/process_sublinks_mutator.md)
  - [negate_clause](../n/negate_clause.md)
  - [process_duplicate_ors](../p/process_duplicate_ors.md)
  - [eval_const_expressions_mutator](../e/eval_const_expressions_mutator.md)
  - [extract_or_clause](../e/extract_or_clause.md)
  - [make_sub_restrictinfos](make_sub_restrictinfos.md)
  - [pgoutput_row_filter_init](../p/pgoutput_row_filter_init.md)

## Notes and Other Information
- The location field is set to -1, indicating that the clause doesn't correspond to a specific location in the original SQL text
- This function is particularly important in bitmap index scan optimization where multiple index conditions are combined with OR logic
- Used extensively in logical replication row filtering and query optimization phases
- The returned Expr pointer can be cast back to BoolExpr when needed for further processing
- Complements `make_andclause` to provide complete boolean expression construction capabilities