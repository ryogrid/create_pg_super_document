# select_common_typmod

## Location
src/backend/parser/parse_coerce.c: 1646 - 1738

## Overview
Determines the common type modifier (typmod) of a list of input expressions that share the same data type.

## Definition
```c
int32 select_common_typmod(ParseState *pstate, List *exprs, Oid common_type)
```

## Detailed Description
This function computes a common type modifier for expressions that have already been determined to share the same data type (typically via select_common_type()). It iterates through all expressions, verifying that each has the specified common_type, and attempts to find a consistent typmod value across all expressions. If all expressions have the same typmod, that value is returned. However, if any expression has a different type than expected, or if the expressions have different typmod values, the function returns -1 (indicating no specific typmod constraint). The typmod represents additional type information such as precision for NUMERIC types or length for VARCHAR types.

## Parameters / Member Variables
- `pstate`: ParseState pointer (currently unused in the function)
- `exprs`: List of expression nodes to analyze for common typmod
- `common_type`: The expected common type OID that all expressions should have

## Dependencies
- Functions called/Symbols referenced:
  - lfirst (list iteration macro)
  - exprType (to get expression type)
  - exprTypmod (to get expression type modifier)

- Called from (representative examples):
  - transformSetOperationTree (set operations like UNION)
  - transformValuesClause (VALUES clause processing)
  - buildMergedJoinVar (join variable creation)
  - analyzeCTE (Common Table Expression analysis)
  - unify_hypothetical_args (hypothetical aggregate argument unification)

## Notes and Other Information
- Returns -1 if expressions don't all have the same typmod or if any expression has wrong type
- Used after select_common_type() to determine if a more specific typmod constraint exists
- Type modifiers provide additional constraints beyond the basic data type (e.g., varchar(50) has typmod 54)
- The pstate parameter is currently unused but maintained for API consistency
- Essential for maintaining type precision in operations like UNION where result column specifications matter
- Located in src/backend/parser/parse_coerce.c:1646-1738