# exprSetInputCollation

## Location
src/backend/nodes/nodeFuncs.c: 1316 - 1379

## Overview
Assigns input collation information to expression tree nodes that support input collation, used during parse analysis for collation-sensitive operations.

## Definition


## Detailed Description
The  function is responsible for setting the input collation field for expression nodes that need to track the collation of their input arguments. Unlike  which sets the result collation, this function sets the collation used for input argument processing.

This function is more selective than , handling only expression types that have an  field and need to know the collation of their input arguments for proper evaluation. It's a no-op for node types that don't store input collation information.

Notably, the function omits  because it requires special treatment since it contains multiple input collation OIDs rather than a single input collation.

## Parameters
- : The expression tree node to assign input collation to
- : The OID of the input collation to assign

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag (to determine expression type)
  - Various expression node type constants (T_Aggref, T_WindowFunc, etc.)

- Called from:
  - [assign_collations_walker](../a/assign_collations_walker.md) (src/backend/parser/parse_collate.c:756, 758)

## Notes and Other Information
- Only handles expression types that have an  field: Aggref, WindowFunc, FuncExpr, OpExpr, DistinctExpr, NullIfExpr, ScalarArrayOpExpr, and MinMaxExpr
- Silently ignores (no-op) for node types that don't store input collation
- Used during collation assignment in parse analysis phase
- Does not handle RowCompareExpr due to its multiple input collation requirements
- Simpler and more focused than exprSetCollation, with only 8 handled node types
- Located in src/backend/nodes/nodeFuncs.c:1316-1379