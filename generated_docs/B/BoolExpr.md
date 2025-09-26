# BoolExpr

## Location
src/include/nodes/primnodes.h: 934 - 942

## Overview
BoolExpr represents boolean logical operations (AND, OR, NOT) in PostgreSQL's expression tree, supporting multiple arguments for AND/OR and exactly one argument for NOT.

## Definition
```c
typedef enum BoolExprType
{
    AND_EXPR, OR_EXPR, NOT_EXPR
} BoolExprType;

typedef struct BoolExpr
{
    pg_node_attr(custom_read_write)

    Expr        xpr;
    BoolExprType boolop;
    List       *args;           /* arguments to this expression */
    ParseLoc    location;       /* token location, or -1 if unknown */
} BoolExpr;
```

## Detailed Description
BoolExpr is the fundamental node type for representing boolean logical operations in PostgreSQL's query tree. It handles the three basic boolean operators: AND, OR, and NOT. The node is designed to be flexible in its argument handling - AND and OR operations can accept two or more arguments (stored as a list), while NOT operations must have exactly one argument.

The node supports PostgreSQL's boolean expression evaluation semantics, including short-circuit evaluation for AND/OR operations. During execution, AND operations return false as soon as any argument evaluates to false, while OR operations return true as soon as any argument evaluates to true.

The custom_read_write attribute indicates that this node has specialized serialization/deserialization logic for plan storage and retrieval. BoolExpr nodes are commonly created during query parsing and transformation, particularly when handling WHERE clauses, JOIN conditions, and other boolean predicates.

## Parameters / Member Variables
- `xpr`: Base Expr node structure containing common expression fields
- `boolop`: Enumerated type indicating the boolean operation (AND_EXPR, OR_EXPR, or NOT_EXPR)
- `args`: List of argument expressions - must contain 2+ elements for AND/OR, exactly 1 element for NOT
- `location`: Parse location of the boolean operator token in the original query, or -1 if location is unknown

## Dependencies
- Functions called/Symbols referenced:
  - BoolExprType
  - ParseLoc
- Called from (representative examples):
  - makeBoolExpr (creates BoolExpr nodes)
  - make_andclause (creates AND expressions)
  - make_orclause (creates OR expressions)
  - make_notclause (creates NOT expressions)
  - transformBoolExpr (during parse analysis)
  - ExecInitExprRec (expression initialization)
  - clause_selectivity_ext (selectivity estimation)
  - negate_clause (query transformation)
  - simplify_or_arguments/simplify_and_arguments (optimization)

## Notes and Other Information
- Supports n-ary operations: AND and OR can have multiple arguments, enabling optimized representation of complex boolean expressions
- NOT operations are strictly unary and must have exactly one argument
- Critical for query optimization, particularly in WHERE clause analysis and predicate pushdown
- Used extensively in constraint processing, join condition analysis, and boolean expression simplification
- The executor implements short-circuit evaluation for efficient boolean expression evaluation
- Helper functions like is_andclause(), is_orclause(), and is_notclause() provide type checking
- Common in query transformation phases where boolean expressions are restructured for optimization
- Location tracking enables accurate error reporting for boolean operator usage errors