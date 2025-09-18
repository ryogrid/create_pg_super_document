# resolveTargetListUnknowns

## Location
src/backend/parser/parse_target.c: 288 - 317

## Overview
Converts any unknown-type targetlist entries to TEXT type as a final fallback after all other type resolution methods have been exhausted.

## Definition
```c
void
resolveTargetListUnknowns(ParseState *pstate, List *targetlist)
```

## Detailed Description
This function serves as a final type resolution step in PostgreSQL's query parsing process. It iterates through a target list and identifies any TargetEntry expressions that still have UNKNOWNOID type after all other type inference mechanisms have been applied. For such entries, it coerces the expression to TEXT type using implicit type conversion. This ensures that all target list entries have concrete types before proceeding to query execution planning.

## Parameters / Member Variables
- `pstate`: ParseState structure containing parser state and context information
- `targetlist`: List of TargetEntry nodes to check and potentially modify for unknown types

## Dependencies
- Functions called/Symbols referenced:
  - exprType
  - [coerce_type](../c/coerce_type.md)
  - lfirst (macro)
  - UNKNOWNOID
  - TEXTOID
  - COERCION_IMPLICIT
  - COERCE_IMPLICIT_CAST
- Called from (representative examples):
  - [transformSelectStmt](../t/transformSelectStmt.md)
  - [transformReturnStmt](../t/transformReturnStmt.md)
  - [transformReturningList](../t/transformReturningList.md)

## Notes and Other Information
- Used as a last resort after all other type resolution methods have been attempted
- Always coerces unknown types to TEXT, which is PostgreSQL's default fallback type
- Uses implicit coercion with COERCE_IMPLICIT_CAST to ensure type safety
- Essential for handling cases where type inference cannot determine concrete types from context
- Modifies the target list in-place by replacing expressions with coerced versions
- Typically called near the end of query transformation after all type inference has been completed