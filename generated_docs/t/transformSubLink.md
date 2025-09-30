# transformSubLink

## Location
[src/backend/parser/parse_expr.c:1772-2014](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L1772-L2014)

## Overview
Transforms a sublink (subquery) node during parsing by validating context appropriateness, analyzing the subquery, and setting up proper parameter handling based on sublink type.

## Definition
```c
static Node *transformSubLink(ParseState *pstate, SubLink *sublink)
```

## Detailed Description
The transformSubLink function handles transformation of subquery expressions during SQL parsing, supporting various sublink types including EXISTS, EXPR, ARRAY, MULTIEXPR, and comparison sublinks (ALL, ANY, ROWCOMPARE). It performs comprehensive validation to ensure subqueries are used in appropriate contexts and sets up the necessary infrastructure for subquery execution.

Key processing steps:
1. **Context validation**: Checks if the sublink is allowed in the current expression context using a comprehensive switch statement
2. **Subquery analysis**: Parses and analyzes the subquery using parse_sub_analyze
3. **Type-specific processing**: Handles different sublink types with appropriate validation and setup:
   - EXISTS: No additional processing needed
   - EXPR/ARRAY: Validates single-column result
   - MULTIEXPR: Used for multi-column assignments
   - Comparison sublinks: Sets up row comparison expressions with PARAM_SUBLINK parameters
4. **Parameter setup**: Creates Param nodes to represent subquery output columns for comparison operations

The function enforces PostgreSQL's subquery usage restrictions, preventing subqueries in contexts like constraints, defaults, and index expressions where they would be inappropriate.

## Parameters / Member Variables
- `pstate`: ParseState context containing parsing state information and error handling context
- `sublink`: SubLink node containing subquery, sublink type, test expression, operators, and location information

## Dependencies
- Functions called/Symbols referenced:
  - [SubLink](../S/SubLink.md), Query, TargetEntry, Param (struct types for subqueries and parameters)
  - RowExpr (struct type for row expressions)
  - Various EXPR_KIND_* constants (expression context validation)
  - Sublink type constants: EXISTS_SUBLINK, EXPR_SUBLINK, ARRAY_SUBLINK, MULTIEXPR_SUBLINK
  - [parse_sub_analyze](../p/parse_sub_analyze.md) (parses and analyzes subqueries)
  - [transformExprRecurse](transformExprRecurse.md) (recursively transforms expressions)
  - [count_nonjunk_tlist_entries](../c/count_nonjunk_tlist_entries.md) (counts non-junk target list entries)
  - [make_row_comparison_op](../m/make_row_comparison_op.md) (creates row comparison expressions)
  - [makeString](../m/makeString.md), makeNode (node creation functions)
  - [exprType](../e/exprType.md), exprTypmod, exprCollation (expression metadata functions)
  - CMD_SELECT (command type constant)
  - PARAM_SUBLINK (parameter type for sublink references)
- Called from:
  - [transformExprRecurse](transformExprRecurse.md) (main expression transformation dispatcher)

## Notes and Other Information
- This function is part of the SQL parser's expression transformation pipeline
- Enforces strict context validation - subqueries are forbidden in many utility contexts like constraints, defaults, and index expressions
- Sets p_hasSubLinks flag to indicate the query contains subqueries
- Handles the conversion of "x IN (select)" syntax to "x = ANY (select)" internally
- Creates PARAM_SUBLINK parameters to represent subquery output columns in comparison operations
- Validates that comparison sublinks have matching numbers of columns between left and right sides
- The comprehensive switch statement ensures compile-time warnings when new expression contexts are added
- The function is static, indicating it's only used within the parse_expr.c module

## Simplified Source

```c
static Node *
transformSubLink(ParseState *pstate, SubLink *sublink)
{
    Node *result = (Node *) sublink;
    Query *qtree;
    const char *err = NULL;

    // Validate sublink is allowed in current expression context
    switch (pstate->p_expr_kind) {
        case EXPR_KIND_WHERE:
        case EXPR_KIND_HAVING:
        case EXPR_KIND_SELECT_TARGET:
        // ... other allowed contexts
            break;
        case EXPR_KIND_CHECK_CONSTRAINT:
            err = "cannot use subquery in check constraint";
            break;
        case EXPR_KIND_COLUMN_DEFAULT:
            err = "cannot use subquery in DEFAULT expression";
            break;
        // ... other forbidden contexts
    }

    if (err)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errmsg_internal("%s", err)));

    pstate->p_hasSubLinks = true;

    // Parse and analyze the subquery
    qtree = parse_sub_analyze(sublink->subselect, pstate, NULL, false, true);

    // Verify it's a SELECT command
    if (!IsA(qtree, Query) || qtree->commandType != CMD_SELECT)
        elog(ERROR, "unexpected non-SELECT command in SubLink");

    sublink->subselect = (Node *) qtree;

    // Handle different sublink types
    if (sublink->subLinkType == EXISTS_SUBLINK) {
        // EXISTS needs no test expression or operators
        sublink->testexpr = NULL;
        sublink->operName = NIL;
    }
    else if (sublink->subLinkType == EXPR_SUBLINK ||
             sublink->subLinkType == ARRAY_SUBLINK) {
        // Ensure single column result
        if (count_nonjunk_tlist_entries(qtree->targetList) != 1)
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                           errmsg("subquery must return only one column")));
        sublink->testexpr = NULL;
        sublink->operName = NIL;
    }
    else if (sublink->subLinkType == MULTIEXPR_SUBLINK) {
        // Multiple columns allowed
        sublink->testexpr = NULL;
        sublink->operName = NIL;
    }
    else {
        // ALL, ANY, or ROWCOMPARE: setup row comparison
        Node *lefthand = transformExprRecurse(pstate, sublink->testexpr);

        // Convert "x IN (select)" to "x = ANY (select)"
        if (sublink->operName == NIL)
            sublink->operName = list_make1(makeString("="));

        // Build parameter list for subquery outputs
        List *right_list = NIL;
        foreach(lc, qtree->targetList) {
            TargetEntry *tent = (TargetEntry *) lfirst(lc);
            if (tent->resjunk)
                continue;

            Param *param = makeNode(Param);
            param->paramkind = PARAM_SUBLINK;
            param->paramid = tent->resno;
            param->paramtype = exprType((Node *) tent->expr);
            // ... set other param fields

            right_list = lappend(right_list, param);
        }

        // Create row comparison expression
        sublink->testexpr = make_row_comparison_op(pstate,
                                                  sublink->operName,
                                                  left_list, right_list,
                                                  sublink->location);
    }

    return result;
}
```