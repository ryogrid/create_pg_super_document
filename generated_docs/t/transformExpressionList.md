# transformExpressionList

## Location
[src/backend/parser/parse_target.c:220-287](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_target.c#L220-L287)

## Overview
Transforms a list of bare expression nodes into a list of transformed expressions, handling star expansion for ROW() and VALUES() constructs without TargetEntry decoration.

## Definition
```c
List *
transformExpressionList(ParseState *pstate, List *exprlist,
                        ParseExprKind exprKind, bool allowDefault)
```

## Detailed Description
This function performs expression transformation similar to transformTargetList, but operates on bare expression nodes without ResTarget decoration and produces bare expressions without TargetEntry decoration. It is specifically designed for ROW() and VALUES() constructs where the simpler expression-only format is needed. The function handles star expansion ("something.*") and provides control over whether SetToDefault nodes are allowed through the allowDefault parameter. Unlike transformTargetList, it does not handle multiassign constructs since they are not expected in this context.

## Parameters / Member Variables
- `pstate`: ParseState structure containing parser state and context information
- `exprlist`: List of expression nodes to be transformed
- `exprKind`: Expression kind constant indicating the transformation context
- `allowDefault`: Boolean flag controlling whether SetToDefault nodes are permitted

## Dependencies
- Functions called/Symbols referenced:
  - [transformExpr](transformExpr.md)
  - [ExpandColumnRefStar](../E/ExpandColumnRefStar.md)
  - [ExpandIndirectionStar](../E/ExpandIndirectionStar.md)
  - [list_concat](../l/list_concat.md)
  - lappend
  - llast
  - IsA (macro)
  - lfirst (macro)
  - [ColumnRef](../C/ColumnRef.md)
  - [A_Indirection](../A/A_Indirection.md)
  - [A_Star](../A/A_Star.md)
  - [SetToDefault](../S/SetToDefault.md)
- Called from (representative examples):
  - [transformInsertStmt](transformInsertStmt.md)
  - [transformValuesClause](transformValuesClause.md)
  - transformRowExpr
  - [transformMergeStmt](transformMergeStmt.md)

## Notes and Other Information
- Simpler version of transformTargetList that works with bare expressions rather than ResTarget/TargetEntry structures
- Specifically designed for ROW() and VALUES() constructs where TargetEntry decoration is not needed
- The allowDefault parameter provides explicit control over SetToDefault handling, which cannot be determined solely from exprKind
- Does not handle multiassign constructs since they are not expected in expression-only contexts
- Star expansion behavior is identical to transformTargetList but produces bare expressions instead of TargetEntry nodes
- When allowDefault is true, SetToDefault nodes pass through unmodified; otherwise they are processed by transformExpr which may generate appropriate errors