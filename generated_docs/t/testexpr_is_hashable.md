# testexpr_is_hashable

## Location
src/backend/optimizer/plan/subselect.c: 761 - 791

## Overview
Determines whether an ANY SubLink's test expression can be implemented using hash-based execution by validating the expression structure and operator hashability.

## Definition
```c
static bool testexpr_is_hashable(Node *testexpr, List *param_ids)
```

## Detailed Description
The `testexpr_is_hashable` function evaluates whether the test expression of an ANY SubLink can be executed using a hash-based approach. This is the counterpart to the memory-based checks performed by `subplan_is_hashable` and `subpath_is_hashable`, focusing instead on whether the expression structure and operators are suitable for hashing.

The function accepts only specific expression patterns that can be efficiently implemented with hash tables:

1. **Single OpExpr**: A simple operator expression that passes the `test_opexpr_is_hashable` check
2. **AND clause of OpExprs**: A Boolean AND expression where each operand is an OpExpr that individually passes the hashability test

For AND clauses, all constituent OpExprs must be hashable for the entire expression to be considered hashable. The function rejects any other expression patterns, including OR clauses, NOT expressions, or complex nested structures that would be difficult to implement efficiently with hash-based lookup.

The `param_ids` parameter is used to distinguish between the left-hand side (LHS) and right-hand side (RHS) of hash expressions, which is essential for proper hash table construction.

## Parameters / Member Variables
- `testexpr`: The test expression node from the ANY SubLink to evaluate for hashability
- `param_ids`: List of output parameter IDs from the SubLink's subquery, used to identify LHS vs RHS in hash expressions

## Dependencies
- Functions called/Symbols referenced:
  - [test_opexpr_is_hashable](test_opexpr_is_hashable.md) (validates individual operator expressions)
  - [is_andclause](../i/is_andclause.md) (checks if expression is a Boolean AND)
- Types referenced:
  - `OpExpr` (operator expression node type)
  - `BoolExpr` (Boolean expression node type)
- Called from (representative examples):
  - [build_subplan](../b/build_subplan.md) (src/backend/optimizer/plan/subselect.c:519)

## Notes and Other Information
This function works together with `subplan_is_hashable`/`subpath_is_hashable` to determine complete feasibility of hash-based ANY SubLink execution. While those functions check memory constraints, this function validates the expression structure. The restriction to OpExpr and AND-of-OpExpr patterns reflects the practical limitations of hash-based execution - more complex expressions would require evaluation for each hash probe, eliminating the performance benefits of hashing. The function is part of PostgreSQL's comprehensive optimization strategy for IN/ANY subqueries, allowing the optimizer to choose the most efficient execution method based on both data characteristics and expression complexity.