# isSimpleNode

## Location
src/backend/utils/adt/ruleutils.c: 8557 - 8785

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
  - FieldSelect
  - FieldStore
  - CoerceToDomain
  - RelabelType
  - CoerceViaIO
  - ArrayCoerceExpr
  - ConvertRowtypeExpr
  - OpExpr
  - PRETTYFLAG_PAREN
  - CoercionForm
  - FuncExpr
  - COERCE_EXPLICIT_CAST
  - COERCE_IMPLICIT_CAST
  - COERCE_SQL_SYNTAX
  - BoolExpr
  - [BoolExprType](../B/BoolExprType.md)
  - NOT_EXPR
  - AND_EXPR
  - OR_EXPR
  - JsonValueExpr
- Called from (representative examples):
  - get_rule_expr_paren
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