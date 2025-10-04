# relabel_to_typmod

## Location
[src/backend/nodes/nodeFuncs.c:684-699](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/nodeFuncs.c#L684-L699)

## Overview
Adds a RelabelType node that changes only the typmod of an expression, preserving its type and collation.

## Definition

```c
Node *
relabel_to_typmod(Node *expr, int32 typmod)
```
## Detailed Description
The  function is a convenience wrapper around  that specifically handles the common case of changing only the type modifier (typmod) of an expression while preserving its base type and collation.

This function is particularly useful in situations where:
- Type support functions need to adjust precision or scale constraints
- Length constraints need to be applied or modified
- The base type remains the same but additional type-specific parameters need to be enforced

The function automatically extracts the current type and collation from the input expression using  and , then calls  with:
- The same type and collation as the original expression
- The new typmod parameter
- COERCE_EXPLICIT_CAST format
- Location set to -1 (unknown)
- overwrite_ok set to false (safe copying)

This ensures that the relabeling operation is both safe and maintains proper expression tree structure.

## Parameters / Member Variables
- `*expr`: The expression node whose typmod should be changed
- `typmod`: The new type modifier value to apply
## Dependencies
- Functions called/Symbols referenced:
  - [applyRelabelType](../a/applyRelabelType.md) (the underlying implementation for relabeling)
  - [exprType](../e/exprType.md) (to extract current type)
  - [exprCollation](../e/exprCollation.md) (to extract current collation)
  - COERCE_EXPLICIT_CAST (coercion format constant)

- Called from (representative examples):
  - Type support functions (numeric_support, interval_support, varchar_support, varbit_support)
  - [TemporalSimplify](../T/TemporalSimplify.md) (for datetime processing)
  - [QTW_EXAMINE_SORTGROUP](../Q/QTW_EXAMINE_SORTGROUP.md) (query tree walker examination)

## Notes and Other Information
- This is a convenience function for a common usage pattern of applyRelabelType
- Always uses COERCE_EXPLICIT_CAST format and -1 location
- Sets overwrite_ok to false for safe operation
- Preserves the original type and collation while only changing typmod
- Commonly used by data type support functions to enforce precision/scale constraints
- The returned node may be the original expression if no relabeling is needed
- Located in src/backend/nodes/nodeFuncs.c:684-699

## Simplified Source

```c
Node *
relabel_to_typmod(Node *expr, int32 typmod)
{
    // Apply relabel with new typmod, preserving type and collation
    return applyRelabelType(expr, exprType(expr), typmod, exprCollation(expr),
                           COERCE_EXPLICIT_CAST, -1, false);
}
```