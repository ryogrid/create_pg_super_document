# checkExprIsVarFree

## Location
[src/backend/parser/parse_clause.c:1925-1949](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_clause.c#L1925-L1949)

## Overview
Validates that a given expression contains no variable references from the current query level, ensuring expressions are constant across all rows.

## Definition

```c
structName)
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
This function is a validation utility used within the PostgreSQL parser to enforce that certain expressions must be variable-free and have consistent values across all rows of a query. It checks whether the provided expression tree contains any Var nodes that reference columns from the current query level (level 0). If such variables are found, the function reports an error with appropriate positioning information. This validation is crucial for constructs like LIMIT clauses and window function frame offsets, where the value must be constant and not depend on individual row values. The function assumes that aggregates and window functions have already been rejected by earlier validation steps.

## Parameters / Member Variables
- : The current parsing state containing context information for error reporting
- : The expression node tree to be checked for variable references
- : A descriptive string identifying the SQL construct being validated, used in error messages

## Dependencies
- Functions called/Symbols referenced:
  - [contain_vars_of_level](contain_vars_of_level.md) (checks for variables at specified query level)
  - [locate_var_of_level](../l/locate_var_of_level.md) (locates first variable at specified query level for error positioning)
  - ereport (error reporting function)
  - [parser_errposition](../p/parser_errposition.md) (provides cursor position in error messages)
- Called from (representative examples):
  - [transformLimitClause](../t/transformLimitClause.md)
  - [transformFrameOffset](../t/transformFrameOffset.md)

## Notes and Other Information
- This is a static function, only accessible within parse_clause.c
- The function specifically checks for level 0 variables (current query level)
- Aggregates and window functions are expected to be rejected by earlier validation
- The function does not reject volatile functions, allowing their first execution value to be used
- Error messages are translatable and include precise cursor positioning
- Essential for maintaining SQL semantic correctness in constructs requiring constant expressions
- The constructName parameter enables context-specific error messages (e.g., 'LIMIT', 'frame offset')
- Used primarily in contexts where expression values must remain consistent across all query rows

## Simplified Source

```c
static void checkExprIsVarFree(ParseState *pstate, Node *expression, const char *constructName) {
    // Check if expression contains variables from current query level
    if (contain_vars_of_level(expression, 0)) {
        // Report error with specific location of the problematic variable
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                 errmsg("argument of %s must not contain variables", constructName),
                 parser_errposition(pstate, locate_var_of_level(expression, 0))));
    }
}
```