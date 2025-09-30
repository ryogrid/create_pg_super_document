# isSimpleNode

## Location
[src/backend/utils/adt/ruleutils.c:8557-8785](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L8557-L8785)

## Overview
Determines whether a given expression node is simple enough to not require parentheses when displayed in the context of its parent node.

## Definition
static bool isSimpleNode(Node *node, Node *parentNode, int prettyFlags)

## Detailed Description
This function implements PostgreSQL's logic for determining when parentheses can be omitted around expression nodes during query deparsing. It evaluates expressions based on their node type and the context provided by their parent node, considering operator precedence rules, SQL syntax conventions, and formatting preferences. The function handles numerous expression types including variables, constants, operators, function calls, boolean expressions, and various coercion nodes. For binary operators, it implements detailed precedence analysis to determine when parentheses are unnecessary, particularly for arithmetic operators like +, -, *, /, and %. The function also considers pretty-printing flags to adjust formatting behavior.

## Parameters / Member Variables
- `node`: The expression node to evaluate for simplicity
- `parentNode`: The parent node that provides context for the evaluation
- `prettyFlags`: Formatting flags that influence parenthesization decisions (e.g., PRETTYFLAG_PAREN)

## Dependencies
- Functions called/Symbols referenced:
  - [get_simple_binary_op_name](../g/get_simple_binary_op_name.md)
  - nodeTag
  - [isSimpleNode](isSimpleNode.md) (recursive calls)
  - [FieldSelect](../F/FieldSelect.md)
  - [FieldStore](../F/FieldStore.md)
  - [CoerceToDomain](../C/CoerceToDomain.md)
  - [RelabelType](../R/RelabelType.md)
  - [CoerceViaIO](../C/CoerceViaIO.md)
  - [ArrayCoerceExpr](../A/ArrayCoerceExpr.md)
  - [ConvertRowtypeExpr](../C/ConvertRowtypeExpr.md)
  - [OpExpr](../O/OpExpr.md)
  - PRETTYFLAG_PAREN
  - CoercionForm
  - [FuncExpr](../F/FuncExpr.md)
  - COERCE_EXPLICIT_CAST
  - COERCE_IMPLICIT_CAST
  - COERCE_SQL_SYNTAX
  - [BoolExpr](../B/BoolExpr.md)
  - [BoolExprType](../B/BoolExprType.md)
  - NOT_EXPR
  - AND_EXPR
  - OR_EXPR
  - [JsonValueExpr](../J/JsonValueExpr.md)
- Called from (representative examples):
  - [get_rule_expr_paren](../g/get_rule_expr_paren.md)
  - [isSimpleNode](isSimpleNode.md) (recursive calls)

## Notes and Other Information
- Returns true if the node is considered simple and doesn't need parentheses
- Returns false for unknown node types ("in dubio complexo" principle)
- Handles complex precedence rules for arithmetic operators (+, -, *, /, %)
- Considers associativity for operators of equal precedence
- Special handling for field access operations to avoid unnecessary parentheses
- Recursively evaluates coercion nodes by checking their underlying arguments
- Implements boolean expression precedence (NOT > AND > OR)
- Accounts for function call contexts, especially for cast operations
- Uses pretty-printing flags to adjust formatting behavior when enabled
- Part of PostgreSQL's expression formatting system for readable SQL output

## Simplified Source

```c
static bool isSimpleNode(Node *node, Node *parentNode, int prettyFlags) {
    if (!node)
        return false;

    switch (nodeTag(node)) {
        // Always simple nodes - single words
        case T_Var:
        case T_Const:
        case T_Param:
        case T_CoerceToDomainValue:
        case T_SetToDefault:
        case T_CurrentOfExpr:
            return true;

        // Function-like expressions - name(..) or name[..]
        case T_SubscriptingRef:
        case T_ArrayExpr:
        case T_RowExpr:
        case T_CoalesceExpr:
        case T_MinMaxExpr:
        case T_SQLValueFunction:
        case T_XmlExpr:
        case T_NextValueExpr:
        case T_NullIfExpr:
        case T_Aggref:
        case T_GroupingFunc:
        case T_WindowFunc:
        case T_MergeSupportFunc:
        case T_FuncExpr:
        case T_JsonConstructorExpr:
        case T_JsonExpr:
        case T_CaseExpr:
            return true;

        // Field access - avoid chained field selections
        case T_FieldSelect:
            return !IsA(parentNode, FieldSelect);
        case T_FieldStore:
            return !IsA(parentNode, FieldStore);

        // Coercion nodes - check underlying expression
        case T_CoerceToDomain:
            return isSimpleNode((Node *) ((CoerceToDomain *) node)->arg, node, prettyFlags);
        case T_RelabelType:
            return isSimpleNode((Node *) ((RelabelType *) node)->arg, node, prettyFlags);
        case T_CoerceViaIO:
            return isSimpleNode((Node *) ((CoerceViaIO *) node)->arg, node, prettyFlags);
        case T_ArrayCoerceExpr:
            return isSimpleNode((Node *) ((ArrayCoerceExpr *) node)->arg, node, prettyFlags);
        case T_ConvertRowtypeExpr:
            return isSimpleNode((Node *) ((ConvertRowtypeExpr *) node)->arg, node, prettyFlags);

        // Complex precedence logic for operators
        case T_OpExpr:
            if (prettyFlags & PRETTYFLAG_PAREN && IsA(parentNode, OpExpr)) {
                const char *op = get_simple_binary_op_name((OpExpr *) node);
                const char *parentOp = get_simple_binary_op_name((OpExpr *) parentNode);

                if (!op || !parentOp) return false;

                bool is_lopriop = (strchr("+-", *op) != NULL);
                bool is_hipriop = (strchr("*/%", *op) != NULL);
                bool is_lopriparent = (strchr("+-", *parentOp) != NULL);
                bool is_hipriparent = (strchr("*/%", *parentOp) != NULL);

                if (!(is_lopriop || is_hipriop) || !(is_lopriparent || is_hipriparent))
                    return false;

                // High precedence op with low precedence parent
                if (is_hipriop && is_lopriparent)
                    return true;
                // Low precedence op with high precedence parent
                if (is_lopriop && is_hipriparent)
                    return false;
                // Same precedence - check if left operand
                return (node == (Node *) linitial(((OpExpr *) parentNode)->args));
            }
            // Fall through to generic logic

        // Generic expression nodes - check parent context
        case T_SubLink:
        case T_NullTest:
        case T_BooleanTest:
        case T_DistinctExpr:
        case T_JsonIsPredicate:
            return (IsA(parentNode, FuncExpr) || IsA(parentNode, BoolExpr) ||
                    IsA(parentNode, SubscriptingRef) || IsA(parentNode, ArrayExpr) ||
                    IsA(parentNode, RowExpr) || IsA(parentNode, CoalesceExpr) ||
                    IsA(parentNode, CaseExpr) || IsA(parentNode, Aggref));

        // Boolean expressions - handle precedence
        case T_BoolExpr:
            if (IsA(parentNode, BoolExpr) && (prettyFlags & PRETTYFLAG_PAREN)) {
                BoolExprType type = ((BoolExpr *) node)->boolop;
                BoolExprType parentType = ((BoolExpr *) parentNode)->boolop;

                // NOT and AND can appear under AND/OR, OR can appear under OR
                return ((type == NOT_EXPR || type == AND_EXPR) &&
                        (parentType == AND_EXPR || parentType == OR_EXPR)) ||
                       (type == OR_EXPR && parentType == OR_EXPR);
            }
            return IsA(parentNode, FuncExpr) || IsA(parentNode, SubscriptingRef) ||
                   IsA(parentNode, ArrayExpr) || IsA(parentNode, CaseExpr);

        // JSON value expressions
        case T_JsonValueExpr:
            return isSimpleNode((Node *) ((JsonValueExpr *) node)->raw_expr, node, prettyFlags);

        default:
            return false; // Unknown nodes are complex
    }
}
```