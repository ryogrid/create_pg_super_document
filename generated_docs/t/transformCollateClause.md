# transformCollateClause

## Location
src/backend/parser/parse_expr.c: 2776 - 2815

## Overview
Handles explicit COLLATE clauses in PostgreSQL by transforming the argument expression and looking up the specified collation name to create a CollateExpr node.

## Definition
```c
static Node *transformCollateClause(ParseState *pstate, CollateClause *c)
```

## Detailed Description
The `transformCollateClause` function processes explicit COLLATE clauses in SQL expressions (e.g., `expr COLLATE "en_US"`). It creates a new CollateExpr node that wraps the transformed argument expression with collation information. The function first transforms the argument expression recursively, then validates that the expression's data type supports collation. It performs a type check to ensure the argument type is collatable, with a special exception for the UNKNOWN type which is handled separately by coerce_type(). After validation, it looks up the specified collation name and stores the collation OID in the CollateExpr node along with location information for error reporting.

## Parameters / Member Variables
- `pstate`: ParseState pointer containing the current parsing context and state information
- `c`: CollateClause pointer containing the argument expression and collation name to be applied

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - transformExprRecurse
  - exprType
  - type_is_collatable
  - LookupCollation
  - format_type_be
  - parser_errposition
- Called from (representative examples):
  - transformExprRecurse

## Notes and Other Information
- The function validates that the argument type supports collation, rejecting non-collatable types except for UNKNOWNOID
- Creates a CollateExpr node that encapsulates both the expression and its collation information
- Preserves location information for accurate error reporting
- The UNKNOWN type receives special handling as it's processed separately by coerce_type()
- Reports clear error messages when attempting to apply collations to non-collatable data types