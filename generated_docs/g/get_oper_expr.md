# get_oper_expr

## Location
[src/backend/utils/adt/ruleutils.c:10425-10464](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L10425-L10464)

## Overview
Parses back an OpExpr (operator expression) node into its human-readable string representation for rule deparsing.

## Definition

```c
static void
get_oper_expr(OpExpr *expr, deparse_context *context)
```
## Detailed Description
This static function is part of PostgreSQL's rule deparsing system, which converts internal expression tree nodes back to SQL text. The function handles both binary operators (like '+', '-', '=') and prefix (unary) operators (like 'NOT', '-'). It formats the operator expression with appropriate spacing and parentheses based on the context's formatting preferences.

The function distinguishes between binary and prefix operators by examining the argument count. For binary operators, it formats the expression as "arg1 operator arg2", while for prefix operators, it uses "operator arg" format. The operator name is resolved using the operator OID and operand types.

## Parameters / Member Variables
- : Pointer to the OpExpr node containing the operator expression to be deparsed
- : Deparse context containing the output buffer and formatting preferences

## Dependencies
- Functions called/Symbols referenced:
  - PRETTY_PAREN (macro for checking parentheses formatting preference)
  - [list_length](../l/list_length.md) (to determine if binary or unary operator)
  - linitial, lsecond (list access macros)
  - [get_rule_expr_paren](get_rule_expr_paren.md) (to recursively deparse operand expressions with parentheses)
  - [generate_operator_name](generate_operator_name.md) (to get the operator's string name from its OID)
  - [exprType](../e/exprType.md) (to determine operand types for operator resolution)
  - [appendStringInfo](../a/appendStringInfo.md), appendStringInfoChar (string buffer operations)
- Called from:
  - [get_rule_expr](get_rule_expr.md) (main expression deparsing dispatcher)

## Notes and Other Information
- This function is part of the larger rule deparsing system used for displaying views, rules, and constraints
- Parentheses are added conditionally based on the PRETTY_PAREN context setting
- The function handles both binary operators (2 arguments) and prefix operators (1 argument)
- Operator name resolution requires both the operator OID and operand types for disambiguation

## Simplified Source

```c
static void get_oper_expr(OpExpr *expr, deparse_context *context) {
    StringInfo buf = context->buf;
    Oid opno = expr->opno;
    List *args = expr->args;

    // Add opening parenthesis if not in pretty mode
    if (!PRETTY_PAREN(context))
        appendStringInfoChar(buf, '(');

    if (list_length(args) == 2) {
        // Binary operator: arg1 operator arg2
        Node *arg1 = (Node *) linitial(args);
        Node *arg2 = (Node *) lsecond(args);

        get_rule_expr_paren(arg1, context, true, (Node *) expr);
        appendStringInfo(buf, " %s ",
                         generate_operator_name(opno,
                                                exprType(arg1),
                                                exprType(arg2)));
        get_rule_expr_paren(arg2, context, true, (Node *) expr);
    } else {
        // Prefix operator: operator arg
        Node *arg = (Node *) linitial(args);

        appendStringInfo(buf, "%s ",
                         generate_operator_name(opno,
                                                InvalidOid,
                                                exprType(arg)));
        get_rule_expr_paren(arg, context, true, (Node *) expr);
    }

    // Add closing parenthesis if not in pretty mode
    if (!PRETTY_PAREN(context))
        appendStringInfoChar(buf, ')');
}
```