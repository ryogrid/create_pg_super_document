# NullTest

## Location
[src/include/nodes/primnodes.h:1955-1963](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L1955-L1963)

## Overview
NullTest represents the operation of testing a value for NULLness in PostgreSQL, implementing both simple NULL tests and row-level NULL checks per SQL standard.

## Definition

```c
typedef struct NullTest
{
	Expr		xpr;
	Expr	   *arg;			/* input expression */
	NullTestType nulltesttype;	/* IS NULL, IS NOT NULL */
	/* T to perform field-by-field null checks */
	bool		argisrow pg_node_attr(query_jumble_ignore);
	ParseLoc	location;		/* token location, or -1 if unknown */
} NullTest;
```
## Detailed Description
NullTest is a node type in PostgreSQL's expression tree that handles NULL testing operations. It supports two distinct modes of operation:

1. **Simple NULL test** (argisrow = false): Performs a standard NULL check on the input expression
2. **Row NULL test** (argisrow = true): Implements "row IS [NOT] NULL" per SQL standard, checking individual fields for NULLness when the row datum itself isn't NULL

The node evaluates the appropriate test and returns a boolean Datum. When argisrow is false with a rowtype input, it represents "row IS [NOT] DISTINCT FROM NULL" rather than the SQL "row IS [NOT] NULL" notation.

## Parameters / Member Variables
- : Base Expr node structure
- : Input expression to be tested for NULL
- : Type of NULL test (IS_NULL or IS_NOT_NULL from NullTestType enum)
- : Boolean flag indicating whether to perform field-by-field null checks for row types
- : Token location in source query, or -1 if unknown

## Dependencies
- Functions called/Symbols referenced:
  - NullTestType (enum with IS_NULL, IS_NOT_NULL values)
  - ParseLoc (for location tracking)
  - Expr (base expression structure)
  
- Called from (representative examples):
  - ExecInitExprRec (executor initialization)
  - clause_selectivity_ext (optimizer selectivity estimation)
  - match_clause_to_indexcol (index matching)
  - transformAExprOp (parser expression transformation)
  - get_rule_expr (rule decompilation)

## Notes and Other Information
- The pg_node_attr(query_jumble_ignore) annotation on argisrow indicates this field should be ignored during query fingerprinting
- Used extensively throughout the optimizer for constraint checking, partition pruning, and selectivity estimation
- Critical for implementing SQL standard NULL semantics, particularly for composite/row types
- The distinction between simple NULL tests and row NULL tests is essential for correct SQL compliance