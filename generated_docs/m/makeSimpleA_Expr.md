# makeSimpleA_Expr

## Location
src/backend/nodes/makefuncs.c: 48 - 65

## Overview
Creates and initializes an A_Expr node with a simple (unqualified) operator name, providing a convenient wrapper around makeA_Expr for common use cases.

## Definition
```c
A_Expr *makeSimpleA_Expr(A_Expr_Kind kind, char *name, Node *lexpr, Node *rexpr, int location)
```

## Detailed Description
The makeSimpleA_Expr function is a specialized constructor utility that creates an A_Expr node specifically for simple, unqualified operator names. It differs from makeA_Expr by accepting a simple string operator name rather than a List, automatically converting the string to a single-element list containing a String node. This function is commonly used throughout the parser when dealing with standard operators that don't require schema qualification.

## Parameters
- `kind`: A_Expr_Kind enum value specifying the type of expression (infix, prefix, postfix, etc.)
- `name`: Character string containing the simple operator name (e.g., "+", "-", "=")
- `lexpr`: Node pointer to the left argument expression, or NULL if not applicable
- `rexpr`: Node pointer to the right argument expression, or NULL if not applicable
- `location`: Integer representing the token location in the source text, or -1 if unknown

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for A_Expr node allocation)
  - list_make1 (creates single-element list)
  - makeString (converts char* to String node)
  - A_Expr (struct type)
  - A_Expr_Kind (enum type)
- Called from (representative examples):
  - transformJoinUsingClause
  - transformAExprBetween
  - transformCaseExpr
  - test_rls_hooks_permissive
  - test_rls_hooks_restrictive

## Notes and Other Information
- Provides a more convenient interface than makeA_Expr for simple operators
- Automatically handles the conversion from string to List of String nodes
- Widely used in parser transformation functions for standard SQL operators
- Part of the makefuncs.c utility collection for node construction