# get_sublink_expr

## Location
[src/backend/utils/adt/ruleutils.c:11490-11614](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L11490-L11614)

## Overview
A static function that deparses PostgreSQL SubLink nodes (subqueries) back into their SQL text representation, handling various sublink types and their associated comparison operators.

## Definition
```c
static void get_sublink_expr(SubLink *sublink, deparse_context *context)
```

## Detailed Description
This function is responsible for converting SubLink nodes back into readable SQL subquery expressions. It handles the complex logic of reconstructing the proper SQL syntax for different types of subqueries including EXISTS, IN/ANY, ALL, row comparisons, and expression sublinks.

The function processes several components:
1. **Sublink type identification**: Determines whether this is an ARRAY, EXISTS, ANY, ALL, ROWCOMPARE, EXPR, MULTIEXPR, or CTE sublink
2. **Test expression parsing**: Handles single operators (OpExpr), multiple operators (BoolExpr), and row comparisons (RowCompareExpr)
3. **Operator name generation**: Uses generate_operator_name() to get the SQL representation of comparison operators
4. **Query deparsing**: Delegates to get_query_def() to handle the actual subquery

The function carefully handles parentheses and operator syntax, including special cases like converting "= ANY" to "IN" for better readability.

## Parameters / Member Variables
- `sublink`: Pointer to the SubLink node containing the subquery and associated metadata
- `context`: Deparsing context containing the output buffer and formatting information

## Dependencies
- Functions called/Symbols referenced:
  - [appendStringInfoString](../a/appendStringInfoString.md)/appendStringInfoChar/appendStringInfo (for buffer operations)
  - [get_rule_expr](get_rule_expr.md) (for expression deparsing)
  - [generate_operator_name](generate_operator_name.md) (for operator name lookup)
  - [get_query_def](get_query_def.md) (for subquery deparsing)
  - IsA (for type checking)
  - linitial/lsecond/linitial_oid (for list operations)
  - lfirst_node (for list iteration)
  - [exprType](../e/exprType.md) (for type information)
  - nodeTag (for node identification)
  - elog (for error reporting)
- Constants referenced:
  - ARRAY_SUBLINK, EXISTS_SUBLINK, ANY_SUBLINK, ALL_SUBLINK
  - ROWCOMPARE_SUBLINK, EXPR_SUBLINK, MULTIEXPR_SUBLINK, CTE_SUBLINK
- Types referenced:
  - [SubLink](../S/SubLink.md), OpExpr, BoolExpr, RowCompareExpr, Query
- Called from:
  - [get_rule_expr](get_rule_expr.md) (for SubLink node processing)

## Notes and Other Information
- This is a static function within ruleutils.c used exclusively for rule deparsing operations
- The function includes a notable limitation: when multiple combining operators are present, only the first operator name is displayed due to SQL syntax constraints
- Special handling is provided for "= ANY" sublinks, which are converted to the more readable "IN" syntax
- The function handles complex parenthesization rules to ensure proper operator precedence
- Error handling is implemented for unrecognized testexpr types and sublink types
- CTE_SUBLINK is explicitly noted as not expected to occur in SubLink contexts
- The function supports both simple scalar subqueries and more complex row-based comparisons

## Simplified Source

```c
static void get_sublink_expr(SubLink *sublink, deparse_context *context)
{
    StringInfo buf = context->buf;
    Query *query = (Query *) (sublink->subselect);
    char *opname = NULL;
    bool need_paren;

    // Start with opening parenthesis (or ARRAY for array sublinks)
    if (sublink->subLinkType == ARRAY_SUBLINK)
        appendStringInfoString(buf, "ARRAY(");
    else
        appendStringInfoChar(buf, '(');

    // Process test expression to extract operator information
    if (sublink->testexpr) {
        if (IsA(sublink->testexpr, OpExpr)) {
            // Single operator case
            OpExpr *opexpr = (OpExpr *) sublink->testexpr;
            get_rule_expr(linitial(opexpr->args), context, true);
            opname = generate_operator_name(opexpr->opno,
                                          exprType(linitial(opexpr->args)),
                                          exprType(lsecond(opexpr->args)));
        }
        else if (IsA(sublink->testexpr, BoolExpr)) {
            // Multiple operators for = or <> cases
            char *sep = "";
            ListCell *l;

            appendStringInfoChar(buf, '(');
            foreach(l, ((BoolExpr *) sublink->testexpr)->args) {
                OpExpr *opexpr = lfirst_node(OpExpr, l);
                appendStringInfoString(buf, sep);
                get_rule_expr(linitial(opexpr->args), context, true);
                if (!opname)
                    opname = generate_operator_name(opexpr->opno,
                                                  exprType(linitial(opexpr->args)),
                                                  exprType(lsecond(opexpr->args)));
                sep = ", ";
            }
            appendStringInfoChar(buf, ')');
        }
        else if (IsA(sublink->testexpr, RowCompareExpr)) {
            // Row comparison cases
            RowCompareExpr *rcexpr = (RowCompareExpr *) sublink->testexpr;
            appendStringInfoChar(buf, '(');
            get_rule_expr((Node *) rcexpr->largs, context, true);
            opname = generate_operator_name(linitial_oid(rcexpr->opnos),
                                          exprType(linitial(rcexpr->largs)),
                                          exprType(linitial(rcexpr->rargs)));
            appendStringInfoChar(buf, ')');
        }
    }

    need_paren = true;

    // Generate appropriate SQL keywords based on sublink type
    switch (sublink->subLinkType) {
        case EXISTS_SUBLINK:
            appendStringInfoString(buf, "EXISTS ");
            break;
        case ANY_SUBLINK:
            if (strcmp(opname, "=") == 0)  // Convert = ANY to IN
                appendStringInfoString(buf, " IN ");
            else
                appendStringInfo(buf, " %s ANY ", opname);
            break;
        case ALL_SUBLINK:
            appendStringInfo(buf, " %s ALL ", opname);
            break;
        case ROWCOMPARE_SUBLINK:
            appendStringInfo(buf, " %s ", opname);
            break;
        case EXPR_SUBLINK:
        case MULTIEXPR_SUBLINK:
        case ARRAY_SUBLINK:
            need_paren = false;
            break;
        default:
            elog(ERROR, "unrecognized sublink type: %d", sublink->subLinkType);
            break;
    }

    // Add inner parenthesis if needed
    if (need_paren)
        appendStringInfoChar(buf, '(');

    // Deparse the subquery itself
    get_query_def(query, buf, context->namespaces, NULL, false,
                  context->prettyFlags, context->wrapColumn,
                  context->indentLevel);

    // Close parentheses
    if (need_paren)
        appendStringInfoString(buf, "))");
    else
        appendStringInfoChar(buf, ')');
}
```