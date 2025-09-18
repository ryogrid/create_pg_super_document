# exprInputCollation

## Location
src/backend/nodes/nodeFuncs.c: 1068 - 1115

## Overview
Returns the OID of the collation that should be used for function input parameters, specifically for expressions that can invoke functions and operators.

## Definition
```c
Oid exprInputCollation(const Node *expr)
```

## Detailed Description
This function extracts the input collation OID from expression nodes that can invoke functions or operators. Unlike exprCollation() which returns the collation of an expression's result, this function returns the collation that should be used when processing the expression's inputs. The input collation represents the resolved common collation of the node's inputs and is what functions and operators should use for their collation-sensitive operations. The function only handles expression types that store inputcollid fields - primarily function calls, operators, aggregates, and window functions. For all other node types, it returns InvalidOid indicating that input collation information is not available or applicable.

## Parameters / Member Variables
- `expr`: The expression node from which to extract input collation information. Can be NULL, in which case InvalidOid is returned.

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag (macro to get node type)
  - Expression node types with inputcollid fields:
    - Aggref (aggregate function reference)
    - WindowFunc (window function)
    - FuncExpr (function call expression)
    - OpExpr (operator expression)
    - DistinctExpr (IS DISTINCT FROM expression)
    - NullIfExpr (NULLIF expression)
    - ScalarArrayOpExpr (scalar-array operator expression)
    - MinMaxExpr (GREATEST/LEAST expression)
  - InvalidOid (constant for invalid OID)

- Called from (representative examples):
  - [check_simple_rowfilter_expr_walker](../c/check_simple_rowfilter_expr_walker.md) (publication row filtering)
  - [resolve_polymorphic_tupdesc](../r/resolve_polymorphic_tupdesc.md) (polymorphic function resolution)

## Notes and Other Information
- This function is complementary to exprCollation() - while exprCollation() returns the collation of the result, this returns the collation for inputs
- Input collation is crucial for functions and operators that need to know how to compare or process their string arguments
- The input collation is typically the resolved common collation derived from all input expressions during collation assignment
- Only a subset of expression node types maintain input collation information, as not all expressions involve collation-sensitive operations
- Returns InvalidOid for expression types that don't store input collation or where it's not applicable
- Important for ensuring consistent collation handling in complex expressions involving multiple collatable inputs
- The inputcollid field is populated during the collation assignment phase of query parsing and analysis