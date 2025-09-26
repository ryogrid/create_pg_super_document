# BooleanTest

## Location
[src/include/nodes/primnodes.h:1979-1985](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L1979-L1985)

## Overview
BooleanTest represents the operation of determining whether a boolean value is TRUE, FALSE, or UNKNOWN (NULL) in PostgreSQL, supporting all six meaningful SQL boolean test combinations.

## Definition

```c
typedef struct BooleanTest
{
	Expr		xpr;
	Expr	   *arg;			/* input expression */
	BoolTestType booltesttype;	/* test type */
	ParseLoc	location;		/* token location, or -1 if unknown */
} BooleanTest;
```
## Detailed Description
BooleanTest is a node type in PostgreSQL's expression tree that handles boolean testing operations according to SQL three-valued logic. Unlike simple boolean evaluation, this node explicitly tests for TRUE, FALSE, or UNKNOWN (NULL) states. A critical aspect is that a NULL input does **not** cause a NULL result - instead, the appropriate test is performed and a definitive boolean Datum is returned.

The node supports all six meaningful boolean test combinations defined by the SQL standard, allowing precise control over three-valued logic behavior in queries.

## Parameters / Member Variables
- : Base Expr node structure
- : Input expression to be tested (typically evaluates to a boolean or NULL)
- : Type of boolean test from BoolTestType enum (IS_TRUE, IS_NOT_TRUE, IS_FALSE, IS_NOT_FALSE, IS_UNKNOWN, IS_NOT_UNKNOWN)
- : Token location in source query, or -1 if unknown

## Dependencies
- Functions called/Symbols referenced:
  - BoolTestType (enum with six test type values)
  - ParseLoc (for location tracking)
  - Expr (base expression structure)
  
- Called from (representative examples):
  - ExecInitExprRec (executor initialization)
  - transformBooleanTest (parser transformation)
  - match_boolean_index_clause (index optimization)
  - clause_selectivity_ext (optimizer selectivity estimation)
  - get_rule_expr (rule decompilation)

## Notes and Other Information
- Essential for implementing SQL standard three-valued logic correctly
- Used in WHERE clauses, CHECK constraints, and conditional expressions where explicit boolean state testing is required
- The six test types cover all combinations: IS TRUE/NOT TRUE, IS FALSE/NOT FALSE, IS UNKNOWN/NOT UNKNOWN
- Critical for partition pruning when dealing with boolean partition keys
- Heavily used in query optimization for boolean index matching and selectivity calculations
- Unlike regular boolean operations, always returns a definitive boolean result even with NULL inputs