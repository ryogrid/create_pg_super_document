# A_Expr

## Location
src/include/nodes/parsenodes.h: 329 - 339

## Overview
A_Expr is a parse tree node representing infix, prefix, and postfix expressions in SQL, handling operators, comparisons, and special SQL constructs like BETWEEN, LIKE, and IN clauses.

## Definition
```c
typedef enum A_Expr_Kind
{
    AEXPR_OP,                   /* normal operator */
    AEXPR_OP_ANY,              /* scalar op ANY (array) */
    AEXPR_OP_ALL,              /* scalar op ALL (array) */
    AEXPR_DISTINCT,            /* IS DISTINCT FROM - name must be "=" */
    AEXPR_NOT_DISTINCT,        /* IS NOT DISTINCT FROM - name must be "=" */
    AEXPR_NULLIF,              /* NULLIF - name must be "=" */
    AEXPR_IN,                  /* [NOT] IN - name must be "=" or "<>" */
    AEXPR_LIKE,                /* [NOT] LIKE - name must be "~~" or "!~~" */
    AEXPR_ILIKE,               /* [NOT] ILIKE - name must be "~~*" or "!~~*" */
    AEXPR_SIMILAR,             /* [NOT] SIMILAR - name must be "~" or "!~" */
    AEXPR_BETWEEN,             /* name must be "BETWEEN" */
    AEXPR_NOT_BETWEEN,         /* name must be "NOT BETWEEN" */
    AEXPR_BETWEEN_SYM,         /* name must be "BETWEEN SYMMETRIC" */
    AEXPR_NOT_BETWEEN_SYM,     /* name must be "NOT BETWEEN SYMMETRIC" */
} A_Expr_Kind;

typedef struct A_Expr
{
    NodeTag     type;
    A_Expr_Kind kind;           /* see above */
    List       *name;           /* possibly-qualified name of operator */
    Node       *lexpr;          /* left argument, or NULL if none */
    Node       *rexpr;          /* right argument, or NULL if none */
    ParseLoc    location;       /* token location, or -1 if unknown */
} A_Expr;
```

## Detailed Description
A_Expr is PostgreSQL's universal representation for expressions involving operators and special SQL constructs. It handles both simple binary operations (like "a + b") and complex SQL-specific constructs (like "x BETWEEN y AND z" or "col IN (list)").

The kind field determines how the expression should be interpreted and processed. For normal operators (AEXPR_OP), the name contains the operator symbol. For special constructs, the kind provides semantic meaning while name contains the canonical representation.

The structure supports both unary operations (with lexpr or rexpr NULL) and binary operations (with both operands present). This flexibility allows A_Expr to represent the full range of SQL expression constructs efficiently.

## Parameters / Member Variables
- `type`: NodeTag identifying this as an A_Expr node
- `kind`: A_Expr_Kind enum specifying the type of expression (operator, BETWEEN, IN, etc.)
- `name`: List of String nodes representing the operator name (possibly schema-qualified)
- `lexpr`: Left operand expression node (NULL for prefix operators)
- `rexpr`: Right operand expression node (NULL for postfix operators)
- `location`: Source location of the expression in the original SQL text

## Dependencies
- Functions called/Symbols referenced:
  - [A_Expr_Kind](A_Expr_Kind.md)
  - ParseLoc
- Called from (representative examples):
  - [transformAExprOp](../t/transformAExprOp.md)
  - [transformAExprOpAny](../t/transformAExprOpAny.md)
  - [transformAExprOpAll](../t/transformAExprOpAll.md)
  - transformAExprDistinct
  - transformAExprNullIf
  - transformAExprIn
  - transformAExprBetween
  - makeA_Expr
  - makeSimpleA_Expr

## Notes and Other Information
- [A_Expr](A_Expr.md) nodes are created by the parser and transformed during semantic analysis
- The kind field determines the specific transformation and resolution logic applied
- Special constructs like BETWEEN are parsed as A_Expr nodes but may be transformed into function calls
- Operator precedence and associativity are handled during parsing before A_Expr creation
- The name field can contain schema-qualified operator names for custom operators
- Used extensively throughout the expression evaluation system
- Marked with custom_read_write attribute for specialized serialization handling
- Essential for representing SQL's rich expression syntax in a uniform parse tree format