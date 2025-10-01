# check_simple_rowfilter_expr_walker

## Location
[src/backend/commands/publicationcmds.c:483-589](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/publicationcmds.c#L483-L589)

## Overview
A tree walker function that validates publication WHERE clause expressions to ensure they contain only allowed constructs for logical replication safety and consistency.

## Definition
```c
static bool check_simple_rowfilter_expr_walker(Node *node, ParseState *pstate)
```

## Detailed Description
This function implements a comprehensive validator for publication row filter expressions by walking through the expression tree and checking each node against strict rules. The function enforces several critical restrictions:

1. **System columns are forbidden** - System columns (varattno < InvalidAttrNumber) are not allowed
2. **User-defined operators are forbidden** - Only built-in operators with OIDs < FirstNormalObjectId are permitted
3. **User-defined types are forbidden** - Only built-in data types are allowed
4. **User-defined functions are forbidden** - Only immutable built-in functions are permitted
5. **User-defined collations are forbidden** - Only built-in collations are allowed

The function supports various node types including operators (OpExpr, DistinctExpr, NullIfExpr, ScalarArrayOpExpr, RowCompareExpr), constants, function expressions, boolean expressions, and various utility expressions. For unsupported node types, it raises an error with detailed information about what's allowed.

## Parameters / Member Variables
- `node`: The current node in the expression tree being validated
- `pstate`: Parse state for error reporting and context

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag
  - InvalidAttrNumber
  - FirstNormalObjectId
  - [contain_mutable_or_user_functions_checker](contain_mutable_or_user_functions_checker.md)
  - [check_functions_in_node](check_functions_in_node.md)
  - [exprType](../e/exprType.md)
  - [exprCollation](../e/exprCollation.md)
  - [exprInputCollation](../e/exprInputCollation.md)
  - [errdetail_internal](../e/errdetail_internal.md)
  - [exprLocation](../e/exprLocation.md)
  - expression_tree_walker
- Called from:
  - [check_simple_rowfilter_expr](check_simple_rowfilter_expr.md)
  - [check_simple_rowfilter_expr_walker](check_simple_rowfilter_expr_walker.md) (recursive)

## Notes and Other Information
- This is a static function used internally within publicationcmds.c
- The function is recursive through expression_tree_walker to validate the entire expression tree
- Restrictions exist to prevent logical decoding failures and security issues with historic snapshots
- System columns are excluded because they aren't replicated to subscribers
- The function provides detailed error messages with specific reasons for rejection
- Supports complex expressions with AND/OR combinations as long as individual components are valid

## Simplified Source

```c
static bool
check_simple_rowfilter_expr_walker(Node *node, ParseState *pstate)
{
    char *errdetail_msg = NULL;

    if (node == NULL)
        return false;

    // Check node type and apply restrictions
    switch (nodeTag(node))
    {
        case T_Var:
            // System columns not allowed
            if (((Var *) node)->varattno < InvalidAttrNumber)
                errdetail_msg = _("System columns are not allowed.");
            break;

        case T_OpExpr:
        case T_DistinctExpr:
        case T_NullIfExpr:
            // Only built-in operators allowed
            if (((OpExpr *) node)->opno >= FirstNormalObjectId)
                errdetail_msg = _("User-defined operators are not allowed.");
            break;

        case T_ScalarArrayOpExpr:
            // Only built-in operators allowed
            if (((ScalarArrayOpExpr *) node)->opno >= FirstNormalObjectId)
                errdetail_msg = _("User-defined operators are not allowed.");
            break;

        case T_RowCompareExpr:
            // Check all operators in row comparison
            foreach(opid, ((RowCompareExpr *) node)->opnos)
            {
                if (lfirst_oid(opid) >= FirstNormalObjectId)
                {
                    errdetail_msg = _("User-defined operators are not allowed.");
                    break;
                }
            }
            break;

        case T_Const:
        case T_FuncExpr:
        case T_BoolExpr:
        case T_RelabelType:
        case T_CollateExpr:
        case T_CaseExpr:
        case T_NullTest:
        case T_List:
            // These node types are supported
            break;

        default:
            errdetail_msg = _("Only columns, constants, built-in operators, built-in data types, built-in collations, and immutable built-in functions are allowed.");
            break;
    }

    // Additional checks for supported nodes
    if (!errdetail_msg && !IsA(node, List))
    {
        // Check for user-defined types
        if (exprType(node) >= FirstNormalObjectId)
            errdetail_msg = _("User-defined types are not allowed.");
        // Check for mutable or user-defined functions
        else if (check_functions_in_node(node, contain_mutable_or_user_functions_checker, pstate))
            errdetail_msg = _("User-defined or built-in mutable functions are not allowed.");
        // Check for user-defined collations
        else if (exprCollation(node) >= FirstNormalObjectId ||
                 exprInputCollation(node) >= FirstNormalObjectId)
            errdetail_msg = _("User-defined collations are not allowed.");
    }

    // Report error if found
    if (errdetail_msg)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("invalid publication WHERE expression"),
                 errdetail_internal("%s", errdetail_msg),
                 parser_errposition(pstate, exprLocation(node))));

    // Continue walking the expression tree
    return expression_tree_walker(node, check_simple_rowfilter_expr_walker, pstate);
}
```