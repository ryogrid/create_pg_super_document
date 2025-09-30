# check_agglevels_and_constraints

## Location
[src/backend/parser/parse_agg.c:299-635](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_agg.c#L299-L635)

## Overview
Validates that aggregate functions and grouping operations are used in appropriate SQL contexts and determines their proper nesting levels within query structures.

## Definition
static void check_agglevels_and_constraints(ParseState *pstate, Node *expr)

## Detailed Description
This function serves as the central validation point for aggregate functions and grouping operations in PostgreSQL. It performs two main tasks: first, it determines the minimum variable level for the aggregate by analyzing its arguments, and second, it validates that the aggregate or grouping operation is being used in a legal SQL context.

The function handles both Aggref nodes (regular aggregates) and GroupingFunc nodes (GROUPING() expressions) uniformly, since they have similar nesting and placement restrictions. It calls check_agg_arguments to analyze the aggregate's arguments and determine at which query nesting level the aggregate should be evaluated. Then it marks the appropriate parse state level as containing aggregates.

The function contains an extensive switch statement that validates the context in which the aggregate appears, checking against numerous expression kinds to ensure SQL standard compliance and PostgreSQL-specific rules. It provides detailed error messages for invalid placements like aggregates in WHERE clauses, JOIN conditions, or various constraint expressions.

## Parameters / Member Variables
- `pstate`: Current parse state containing context information and expression kind
- `expr`: Node representing either an Aggref or GroupingFunc to be validated

## Dependencies
- Functions called/Symbols referenced:
  - [check_agg_arguments](check_agg_arguments.md)
  - [ParseExprKindName](../P/ParseExprKindName.md)
  - ereport (for error handling)
- Called from (representative examples):
  - [transformAggregateCall](../t/transformAggregateCall.md)
  - [transformGroupingFunc](../t/transformGroupingFunc.md)
  - check_ungrouped_columns_context

## Notes and Other Information
- The function treats both aggregate functions and GROUPING operations identically for validation purposes
- Contains comprehensive coverage of all SQL expression contexts where aggregates might appear
- Uses two error reporting schemes: custom messages for complex contexts and generic messages with ParseExprKindName for simple keyword contexts
- The switch statement intentionally has no default case to ensure compiler warnings when new expression kinds are added
- Properly handles query nesting by walking up the parse state hierarchy to mark the correct level as having aggregates

## Simplified Source

```c
static void
check_agglevels_and_constraints(ParseState *pstate, Node *expr) {
    List *directargs = NIL;
    List *args = NIL;
    Expr *filter = NULL;
    int min_varlevel;
    int location = -1;
    Index *p_levelsup;
    const char *err = NULL;
    bool errkind = false;
    bool isAgg = IsA(expr, Aggref);

    // Extract arguments and location based on expression type
    if (isAgg) {
        Aggref *agg = (Aggref *) expr;
        directargs = agg->aggdirectargs;
        args = agg->args;
        filter = agg->aggfilter;
        location = agg->location;
        p_levelsup = &agg->agglevelsup;
    } else {
        GroupingFunc *grp = (GroupingFunc *) expr;
        args = grp->args;
        location = grp->location;
        p_levelsup = &grp->agglevelsup;
    }

    // Determine the nesting level for this aggregate
    min_varlevel = check_agg_arguments(pstate, directargs, args, filter);
    *p_levelsup = min_varlevel;

    // Mark the appropriate parse state level as having aggregates
    while (min_varlevel-- > 0) {
        pstate = pstate->parentParseState;
    }
    pstate->p_hasAggs = true;

    // Check if aggregate is in a valid context
    switch (pstate->p_expr_kind) {
        case EXPR_KIND_NONE:
            Assert(false);  // Should not happen
            break;

        // Allow these contexts
        case EXPR_KIND_OTHER:
        case EXPR_KIND_HAVING:
        case EXPR_KIND_WINDOW_PARTITION:
        case EXPR_KIND_WINDOW_ORDER:
        case EXPR_KIND_SELECT_TARGET:
        case EXPR_KIND_ORDER_BY:
        case EXPR_KIND_DISTINCT_ON:
            break;

        // Contexts that use generic error messages
        case EXPR_KIND_WHERE:
        case EXPR_KIND_FILTER:
        case EXPR_KIND_INSERT_TARGET:
        case EXPR_KIND_UPDATE_SOURCE:
        case EXPR_KIND_UPDATE_TARGET:
        case EXPR_KIND_GROUP_BY:
        case EXPR_KIND_LIMIT:
        case EXPR_KIND_OFFSET:
        case EXPR_KIND_RETURNING:
        case EXPR_KIND_MERGE_RETURNING:
        case EXPR_KIND_VALUES:
        case EXPR_KIND_VALUES_SINGLE:
        case EXPR_KIND_CYCLE_MARK:
            errkind = true;
            break;

        // Contexts with custom error messages
        case EXPR_KIND_JOIN_ON:
        case EXPR_KIND_JOIN_USING:
            err = isAgg ? "aggregate functions are not allowed in JOIN conditions"
                        : "grouping operations are not allowed in JOIN conditions";
            break;

        case EXPR_KIND_FROM_SUBSELECT:
            err = isAgg ? "aggregate functions are not allowed in FROM clause"
                        : "grouping operations are not allowed in FROM clause";
            break;

        // ... (many other specific error cases)
        default:
            // Additional cases handled with specific error messages
            if (/* various constraint contexts */) {
                err = isAgg ? "aggregate functions not allowed in constraints"
                            : "grouping operations not allowed in constraints";
            }
            break;
    }

    // Report errors
    if (err) {
        ereport(ERROR, (errcode(ERRCODE_GROUPING_ERROR),
                       errmsg_internal("%s", err),
                       parser_errposition(pstate, location)));
    }

    if (errkind) {
        err = isAgg ? "aggregate functions are not allowed in %s"
                    : "grouping operations are not allowed in %s";
        ereport(ERROR, (errcode(ERRCODE_GROUPING_ERROR),
                       errmsg_internal(err, ParseExprKindName(pstate->p_expr_kind)),
                       parser_errposition(pstate, location)));
    }
}
```