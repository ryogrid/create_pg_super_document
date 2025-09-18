# is_standard_join_alias_expression

## Location
src/backend/optimizer/util/var.c: 962 - 1035

## Overview
Determines whether a join alias expression can accommodate direct nullingrels integration without requiring a PlaceHolderVar wrapper.

## Definition
```c
static bool is_standard_join_alias_expression(Node *newnode, Var *oldvar)
```

## Detailed Description
This function performs a structural analysis of join alias expressions to determine if they can be modified in-place to carry nullingrels information. The function implements a recursive descent parser that recognizes a specific set of "standard" expression patterns that the PostgreSQL parser commonly generates for join aliases.

**Accepted Expression Types**:

1. **Var**: Simple variable references at the correct query level
2. **PlaceHolderVar**: Placeholder variables at the correct query level  
3. **FuncExpr**: Function expressions, but only those representing implicit coercions (COERCE_IMPLICIT_CAST format) with at least one argument
4. **RelabelType**: Type relabeling operations that preserve null behavior
5. **CoerceViaIO**: Input/output coercions that preserve null behavior
6. **ArrayCoerceExpr**: Array coercions that preserve null behavior at the array level
7. **CoalesceExpr**: COALESCE expressions where all arguments are standard expressions

**Key Design Principles**:
- Only accepts expressions that won't produce non-NULL from NULL inputs
- Recursively validates nested structures (e.g., coercion arguments, COALESCE operands)
- Ensures proper query level matching between expressions and the original Var
- Focuses on the first argument of coercions (additional arguments are typically constants)

The function serves as a gatekeeper for the optimization where nullingrels can be directly integrated into expression nodes rather than requiring a more expensive PlaceHolderVar wrapper.

## Parameters / Member Variables
- `newnode`: The join alias expression to be analyzed
- `oldvar`: The original Var being replaced, used for query level validation

## Dependencies
- Functions called/Symbols referenced:
  - PlaceHolderVar, FuncExpr, RelabelType
  - CoerceViaIO, ArrayCoerceExpr, CoalesceExpr
  - COERCE_IMPLICIT_CAST
  - linitial (for examining first arguments)
  - Recursive calls to is_standard_join_alias_expression
- Called from (representative examples):
  - add_nullingrels_if_needed (to determine integration strategy)

## Notes and Other Information
- Returns false for NULL input or unrecognized node types
- The function is conservative - expressions that might be safe but aren't explicitly recognized return false
- Query level matching (varlevelsup/phlevelsup) is essential for correct variable scope handling
- The restriction to implicit coercions helps ensure null-preservation semantics
- COALESCE expressions require all arguments to be standard expressions for the entire expression to qualify
- The function's coverage is designed to handle anything the parser would put into joinaliasvars