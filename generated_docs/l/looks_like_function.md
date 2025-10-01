# looks_like_function

## Location
[src/backend/utils/adt/ruleutils.c:10396-10424](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L10396-L10424)

## Overview
A helper function that determines whether a parse tree node will deparse as a function-like expression that satisfies PostgreSQL's func_expr_windowless grammar rule.

## Definition

```c
static bool
looks_like_function(Node *node)
```
## Detailed Description
 is a utility function that examines a parse tree node and determines whether it will produce output that matches PostgreSQL's  grammar production when deparsed. This function is crucial for maintaining grammatical correctness in contexts where function-like syntax is specifically required.

The function performs a switch-case analysis on the node type:

- **FuncExpr**: Returns true only if the function format is  or , but not if it would deparse as a cast
- **NullIfExpr, CoalesceExpr, MinMaxExpr, SQLValueFunction, XmlExpr, JsonExpr**: All return true as they are accepted by  grammar rules
- **All other node types**: Return false

The function adopts a conservative approach - when in doubt, it returns false, which is always safe since calling code can then apply appropriate wrapping (like CAST()) to ensure grammatical compliance.

## Parameters / Member Variables
- : The parse tree node to examine (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag (for node type identification)
  - COERCE_EXPLICIT_CALL, COERCE_SQL_SYNTAX (coercion format constants)
  - Various node type constants (T_FuncExpr, T_NullIfExpr, etc.)

- Called from (representative examples):
  - [get_rule_expr_funccall](../g/get_rule_expr_funccall.md)
  - [pg_get_indexdef_worker](../p/pg_get_indexdef_worker.md)
  - [pg_get_statisticsobj_worker](../p/pg_get_statisticsobj_worker.md)
  - [pg_get_partkeydef_worker](../p/pg_get_partkeydef_worker.md)

## Notes and Other Information
- Critical for maintaining SQL grammar compliance in deparsing operations
- Conservative design ensures safety - false negatives are acceptable, false positives are not
- Part of PostgreSQL's sophisticated rule system for accurate SQL reconstruction
- Helps distinguish between expressions that naturally look like functions vs. those that need wrapping
- Essential for contexts like index definitions and partition key definitions where function-like syntax is required

## Simplified Source

```c
static bool
looks_like_function(Node *node)
{
    if (node == NULL)
        return false;  // Shouldn't happen but safe to handle

    switch (nodeTag(node))
    {
        case T_FuncExpr:
            // OK unless it will deparse as a cast
            return (((FuncExpr *) node)->funcformat == COERCE_EXPLICIT_CALL ||
                    ((FuncExpr *) node)->funcformat == COERCE_SQL_SYNTAX);

        case T_NullIfExpr:
        case T_CoalesceExpr:
        case T_MinMaxExpr:
        case T_SQLValueFunction:
        case T_XmlExpr:
        case T_JsonExpr:
            // These are all accepted by func_expr_common_subexpr
            return true;

        default:
            break;
    }

    return false;  // Conservative: when in doubt, return false
}
```