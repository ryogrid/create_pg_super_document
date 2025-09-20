# CRERR_TOO_MANY

## Location
[src/backend/parser/parse_expr.c:523-885](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L523-L885)

## Overview
 is an enum value used within the transformColumnRef function to indicate that a column reference has too many qualification levels.

## Definition

```c
CRERR_WRONG_DB,
		CRERR_TOO_MANY
	}			crerr = CRERR_NO_COLUMN;
	const char *err;

	/*
	 * Check to see if the column reference is in an invalid place within the
	 * query.  We allow column references in most places, except in default
	 * expressions and partition bound expressions.
	 */
	err = NULL;
	switch (pstate->p_expr_kind)
```
## Detailed Description
 is one of four enum values defined locally within the  function to categorize different types of column reference errors. This particular value indicates that the column reference contains more qualification levels than PostgreSQL supports. PostgreSQL supports up to 4 levels of qualification in column references: catalog.schema.table.column. When a column reference exceeds this limit, the error tracking variable  is set to , which later triggers an appropriate error message indicating that the column reference has too many dots (qualification levels). This enum-based approach allows the function to defer error reporting until after all parsing attempts have been made, providing more informative error messages to users.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - None (this is an enum constant)
  
- Called from (representative examples):
  - Used within transformColumnRef function for error categorization
  - Referenced in error handling logic when list_length(cref->fields) > 4

## Notes and Other Information
- This is a local enum defined only within the scope of the  function in src/backend/parser/parse_expr.c:523
- Part of a comprehensive error categorization system that includes CRERR_NO_COLUMN, CRERR_NO_RTE, CRERR_WRONG_DB, and CRERR_TOO_MANY
- PostgreSQL's maximum supported qualification depth is 4 levels (catalog.schema.table.column)
- The error tracking approach allows for deferred error reporting with context-specific error messages
- Used in conjunction with switch statements that process column references of different qualification lengths (1-4 parts)
- When this error condition is detected, it ultimately results in a user-facing error message about excessive qualification levels