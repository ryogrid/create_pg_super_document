# transformMergeSupportFunc

## Location
src/backend/parser/parse_expr.c: 1378 - 1402

## Overview
Validates that MergeSupportFunc nodes (MERGE_ACTION() expressions) are only used in appropriate contexts and returns them unchanged if valid.

## Definition
```c
static Node *transformMergeSupportFunc(ParseState *pstate, MergeSupportFunc *f)
```

## Detailed Description
This function handles the transformation of MergeSupportFunc nodes, which represent the MERGE_ACTION() function in SQL MERGE statements. Unlike other transformation functions, this one primarily performs context validation rather than structural transformation.

The function implements a strict context validation mechanism:

**1. Context Validation**:
   - Checks if the current parse state indicates we're in a MERGE RETURNING clause (EXPR_KIND_MERGE_RETURNING)
   - If not in the correct context, searches up the parent parse state chain to find a valid MERGE RETURNING context
   - This hierarchical search handles nested parsing contexts like subqueries within MERGE statements

**2. Error Reporting**:
   - If no valid MERGE RETURNING context is found at any level, reports a syntax error
   - Provides a clear error message indicating that MERGE_ACTION() can only be used in MERGE RETURNING lists
   - Includes precise location information for error reporting

**3. Pass-Through Transformation**:
   - If validation passes, returns the MergeSupportFunc node unchanged
   - The actual functionality of MERGE_ACTION() is handled during execution, not parsing

The MERGE_ACTION() function is a special PostgreSQL function that returns information about the action performed by a MERGE statement (INSERT, UPDATE, or DELETE), and it's only meaningful within the RETURNING clause of MERGE commands.

## Parameters / Member Variables
- `pstate`: ParseState context containing parsing state and environment information, including expression context kind
- `f`: MergeSupportFunc node representing the MERGE_ACTION() function call to validate

## Dependencies
- Functions called/Symbols referenced:
  - EXPR_KIND_MERGE_RETURNING (constant for context checking)
  - ereport (for error reporting)
  - errcode
  - errmsg
  - parser_errposition
- Called from (representative examples):
  - transformExprRecurse

## Notes and Other Information
- The function is static, meaning it's only accessible within parse_expr.c
- This is primarily a validation function rather than a transformation function
- The hierarchical parent parse state search handles complex nested query scenarios
- MERGE_ACTION() is a PostgreSQL-specific extension for MERGE statement introspection
- The validation ensures type safety and prevents misuse of the special function
- Located in src/backend/parser/parse_expr.c:1378-1402