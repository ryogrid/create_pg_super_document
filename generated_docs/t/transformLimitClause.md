# transformLimitClause

## Location
[src/backend/parser/parse_clause.c:1881-1924](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_clause.c#L1881-L1924)

## Overview
Transforms SQL LIMIT and OFFSET clause expressions into internal expression trees, ensuring they are of type bigint and meet semantic requirements.

## Definition

```c
structName,
					 LimitOption limitOption)
{
	Node	   *qual;

	if (clause == NULL)
		return NULL;

	qual = transformExpr(pstate, clause, exprKind);

	qual = coerce_to_specific_type(pstate, qual, INT8OID, constructName);

	/* LIMIT can't refer to any variables of the current query */
	checkExprIsVarFree(pstate, qual, constructName);

	/*
	 * Don't allow NULLs in FETCH FIRST .. WITH TIES.  This test is ugly and
	 * extremely simplistic, in that you can pass a NULL anyway by hiding it
	 * inside an expression -- but this protects ruleutils against emitting an
	 * unadorned NULL that's not accepted back by the grammar.
	 */
	if (exprKind == EXPR_KIND_LIMIT && limitOption == LIMIT_OPTION_WITH_TIES &&
		IsA(clause, A_Const) && castNode(A_Const, clause)->isnull)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE),
				 errmsg("row count cannot be null in FETCH FIRST ... WITH TIES clause")));

	return qual;
}

/*
 * checkExprIsVarFree
 *		Check that given expr has no Vars of the current query level
 *		(aggregates and window functions should have been rejected already).
 *
 * This is used to check expressions that have to have a consistent value
 * across all rows of the query, such as a LIMIT.  Arguably it should reject
 * volatile functions, too, but we don't do that --- whatever value the
 * function gives on first execution is what you get.
 *
 * constructName does not affect the semantics, but is used in error messages
 */
static void
checkExprIsVarFree(ParseState *pstate, Node *n, const char *constructName)
{
	if (contain_vars_of_level(n, 0))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
		/* translator: %s is name of a SQL construct, eg LIMIT */
				 errmsg("argument of %s must not contain variables",
						constructName),
				 parser_errposition(pstate,
									locate_var_of_level(n, 0))));
	}
}


/*
 * checkTargetlistEntrySQL92 -
 *	  Validate a targetlist entry found by findTargetlistEntrySQL92
 *
 * When we select a pre-existing tlist entry as a result of syntax such
 * as "GROUP BY 1", we have to make sure it is acceptable for use in the
 * indicated clause type;
```
## Detailed Description
This function is responsible for processing LIMIT and OFFSET clauses in SQL SELECT statements and related constructs. It performs several critical transformations and validations: first, it calls transformExpr to convert the raw clause into a proper expression tree, then coerces the result to INT8 (bigint) type as required by PostgreSQL's LIMIT implementation since version 8.2. The function also enforces that LIMIT expressions cannot reference variables from the current query level by calling checkExprIsVarFree. Additionally, it includes special validation for FETCH FIRST ... WITH TIES constructs to prevent NULL values, which would cause issues in query rule generation.

## Parameters / Member Variables
- : The current parsing state containing context information for the transformation
- : The raw parse tree node representing the LIMIT/OFFSET expression to be transformed  
- : An enumeration value specifying the expression context (EXPR_KIND_LIMIT, etc.)
- : A descriptive string used in error messages to identify the SQL construct
- : An enumeration specifying the type of limit option (WITH TIES, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [transformExpr](transformExpr.md) (expression transformation)
  - [coerce_to_specific_type](../c/coerce_to_specific_type.md) (type coercion to INT8)
  - [checkExprIsVarFree](../c/checkExprIsVarFree.md) (variable reference validation)
  - [ParseExprKind](../P/ParseExprKind.md) (enumeration type)
  - [LimitOption](../L/LimitOption.md) (enumeration type)
  - [A_Const](../A/A_Const.md) (constant node type)
  - LIMIT_OPTION_WITH_TIES (enum value)
  - EXPR_KIND_LIMIT (enum value)
- Called from (representative examples):
  - [transformSelectStmt](transformSelectStmt.md)
  - [transformValuesClause](transformValuesClause.md)
  - [transformSetOperationStmt](transformSetOperationStmt.md)
  - [transformPLAssignStmt](transformPLAssignStmt.md)

## Notes and Other Information
- Returns NULL if the input clause is NULL, making LIMIT/OFFSET clauses optional
- Since PostgreSQL 8.2, LIMIT expressions must be INT8 (bigint) rather than INT4 (integer)
- The function prevents variable references in LIMIT clauses to ensure they are constant expressions
- Special validation exists for FETCH FIRST ... WITH TIES to prevent NULL literals that would break query rule generation
- The constructName parameter is used purely for error reporting
- This function is declared in parse_clause.h and used throughout the parser for various SELECT-related constructs
- The checkExprIsVarFree call ensures LIMIT values are deterministic and don't depend on query results