# transformMergeSupportFunc

## Location
[src/backend/parser/parse_expr.c:1378-1402](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L1378-L1402)

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
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - [parser_errposition](../p/parser_errposition.md)
- Called from (representative examples):
  - [transformExprRecurse](transformExprRecurse.md)

## Notes and Other Information
- The function is static, meaning it's only accessible within parse_expr.c
- This is primarily a validation function rather than a transformation function
- The hierarchical parent parse state search handles complex nested query scenarios
- MERGE_ACTION() is a PostgreSQL-specific extension for MERGE statement introspection
- The validation ensures type safety and prevents misuse of the special function
- Located in src/backend/parser/parse_expr.c:1378-1402

## Simplified Source

```c
static Node *transformMergeSupportFunc(ParseState *pstate, MergeSupportFunc *f) {
    // Check if we're in the correct context (MERGE RETURNING clause)
    if (pstate->p_expr_kind != EXPR_KIND_MERGE_RETURNING) {
        ParseState *parent_pstate = pstate->parentParseState;

        // Search up the parent chain for valid MERGE RETURNING context
        while (parent_pstate &&
               parent_pstate->p_expr_kind != EXPR_KIND_MERGE_RETURNING) {
            parent_pstate = parent_pstate->parentParseState;
        }

        // If no valid context found, report error
        if (!parent_pstate) {
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                           errmsg("MERGE_ACTION() can only be used in the RETURNING list of a MERGE command"),
                           parser_errposition(pstate, f->location)));
        }
    }

    // Return the node unchanged - actual processing happens at execution
    return (Node *) f;
}
```