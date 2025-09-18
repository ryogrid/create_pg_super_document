# check_simple_rowfilter_expr_walker

## Location
src/backend/commands/publicationcmds.c: 483 - 589

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
  - check_functions_in_node
  - exprType
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