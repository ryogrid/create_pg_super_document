# check_srf_call_placement

## Location
[src/backend/parser/parse_func.c:2511-2682](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_func.c#L2511-L2682)

## Overview
check_srf_call_placement validates that set-returning functions (SRFs) are called in syntactically valid locations within SQL queries and sets appropriate parser state flags for query planning.

## Definition
```c
void check_srf_call_placement(ParseState *pstate, Node *last_srf, int location)
```

## Detailed Description
This function enforces PostgreSQL's restrictions on where set-returning functions can be used within SQL statements. It examines the current parsing context (stored in pstate->p_expr_kind) to determine if an SRF call is valid in the current location. Valid locations include SELECT target lists, GROUP BY/ORDER BY expressions, window partition/order clauses, and function calls in FROM clauses. The function prevents SRFs from being used in inappropriate contexts like WHERE clauses, JOIN conditions, CHECK constraints, and DEFAULT expressions. When SRFs are found in valid target contexts, it sets pstate->p_hasTargetSRFs to true, which affects query planning decisions. For FROM function contexts, it additionally checks for nested SRFs which are not supported.

## Parameters / Member Variables
- `pstate`: ParseState containing current parsing context and expression kind information
- `last_srf`: Snapshot of pstate->p_last_srf from before parsing function arguments, used to detect nested SRFs
- `location`: Source location of the SRF call for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - [errmsg_internal](../e/errmsg_internal.md)
  - [parser_errposition](../p/parser_errposition.md)
  - [exprLocation](../e/exprLocation.md)
  - [ParseExprKindName](../P/ParseExprKindName.md)
  - ERRCODE_FEATURE_NOT_SUPPORTED
  - Various EXPR_KIND_* constants
- Called from (representative examples):
  - [ParseFuncOrColumn](../P/ParseFuncOrColumn.md)
  - Function call transformation code
  - Expression parsing functions

## Notes and Other Information
The function uses a comprehensive switch statement covering all possible ParseExprKind values to ensure complete coverage as new expression contexts are added to PostgreSQL. It employs two error reporting mechanisms: custom error messages for complex contexts and generic messages using ParseExprKindName for simple SQL keyword contexts. The p_hasTargetSRFs flag set by this function is crucial for query planning as it indicates that the query will produce multiple result rows per input row. The nested SRF detection prevents complex cases that would be difficult to handle during execution, requiring SRFs in FROM clauses to appear at the top level only.

## Simplified Source

```c
void check_srf_call_placement(ParseState *pstate, Node *last_srf, int location) {
    const char *err = NULL;
    bool errkind = false;

    // Check if SRF is in valid context based on expression kind
    switch (pstate->p_expr_kind) {
        // Valid contexts that allow SRFs
        case EXPR_KIND_SELECT_TARGET:
        case EXPR_KIND_INSERT_TARGET:
        case EXPR_KIND_GROUP_BY:
        case EXPR_KIND_ORDER_BY:
        case EXPR_KIND_DISTINCT_ON:
        case EXPR_KIND_WINDOW_PARTITION:
        case EXPR_KIND_WINDOW_ORDER:
        case EXPR_KIND_VALUES_SINGLE:
            pstate->p_hasTargetSRFs = true;
            break;

        case EXPR_KIND_FROM_FUNCTION:
            // Special case: check for nested SRFs (not allowed)
            if (pstate->p_last_srf != last_srf) {
                ereport(ERROR, "set-returning functions must appear at top level of FROM");
            }
            break;

        case EXPR_KIND_OTHER:
            // Let caller decide if this is valid
            break;

        // Contexts that use standard error messages
        case EXPR_KIND_WHERE:
        case EXPR_KIND_HAVING:
        case EXPR_KIND_FILTER:
        case EXPR_KIND_LIMIT:
        case EXPR_KIND_OFFSET:
        case EXPR_KIND_UPDATE_SOURCE:
        case EXPR_KIND_UPDATE_TARGET:
        case EXPR_KIND_RETURNING:
        case EXPR_KIND_VALUES:
            errkind = true;
            break;

        // Contexts with custom error messages
        case EXPR_KIND_JOIN_ON:
        case EXPR_KIND_JOIN_USING:
            err = "set-returning functions are not allowed in JOIN conditions";
            break;

        case EXPR_KIND_POLICY:
            err = "set-returning functions are not allowed in policy expressions";
            break;

        case EXPR_KIND_CHECK_CONSTRAINT:
        case EXPR_KIND_DOMAIN_CHECK:
            err = "set-returning functions are not allowed in check constraints";
            break;

        case EXPR_KIND_COLUMN_DEFAULT:
        case EXPR_KIND_FUNCTION_DEFAULT:
            err = "set-returning functions are not allowed in DEFAULT expressions";
            break;

        default:
            // Most other contexts are not allowed
            err = "set-returning functions are not allowed in this context";
            break;
    }

    // Report errors if found
    if (err) {
        ereport(ERROR, errmsg_internal("%s", err));
    }
    if (errkind) {
        ereport(ERROR, errmsg("set-returning functions are not allowed in %s",
                             ParseExprKindName(pstate->p_expr_kind)));
    }
}
```