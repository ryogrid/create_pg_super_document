# TidExpr

## Location
src/backend/executor/nodeTidscan.c: 49 - 54

## Overview
TidExpr is a structure used in PostgreSQL TID (Tuple Identifier) scan operations to represent one element in a list of expressions that yield TID values for direct tuple access.

## Definition
```c
typedef struct TidExpr
{
    ExprState  *exprstate;        /* ExprState for a TID-yielding subexpr */
    bool        isarray;          /* if true, it yields tid[] not just tid */
    CurrentOfExpr *cexpr;         /* alternatively, we can have CURRENT OF */
} TidExpr;
```

## Detailed Description
TidExpr is a fundamental data structure used in TID scan operations within PostgreSQL execution engine. It represents one element in the `tss_tidexprs` list and encapsulates different ways to obtain TID values for direct tuple access. The structure supports three distinct modes of operation: single TID expressions, array TID expressions, and CURRENT OF cursor expressions. This flexibility allows the TID scan executor to handle various forms of TID-based queries efficiently.

## Parameters / Member Variables
- `exprstate`: Points to an ExprState structure containing the compiled expression that yields TID values. This field is used for both single TID and array TID expressions.
- `isarray`: Boolean flag indicating whether the expression yields an array of TIDs (tid[]) or a single TID value. When true, the expression evaluation will return multiple TID values.
- `cexpr`: Alternative to exprstate, this field contains a CurrentOfExpr for handling "CURRENT OF cursor_name" clauses in SQL queries.

## Dependencies
- Functions called/Symbols referenced:
  - [CurrentOfExpr](../C/CurrentOfExpr.md) (alternative expression type for cursor-based TID access)
  - ExprState (expression state for compiled TID expressions)
- Called from (representative examples):
  - TidListEval (evaluates TidExpr list to compute TIDs for scanning)
  - [TidExprListCreate](TidExprListCreate.md) (creates and initializes TidExpr structures)

## Notes and Other Information
- [TidExpr](TidExpr.md) is specifically designed for TID scan operations where direct tuple access is needed based on physical tuple identifiers
- The structure supports both simple expressions that return single TIDs and more complex expressions that return arrays of TIDs
- CURRENT OF functionality allows referencing the current position of a cursor, providing an alternative way to specify which tuple to access
- Used internally by the PostgreSQL executor for optimizing queries that can benefit from direct tuple access via TID values
- Located in src/backend/executor/nodeTidscan.c as part of the TID scan execution node implementation