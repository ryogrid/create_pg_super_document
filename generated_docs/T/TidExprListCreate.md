# TidExprListCreate

## Location
[src/backend/executor/nodeTidrangescan.c:106-136](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeTidrangescan.c#L106-L136)

## Overview
TidExprListCreate extracts and processes TID qualification expressions from a TidRangeScan plan node, converting them into a list of TidOpExpr structures for efficient TID range scanning.

## Definition

```c
static void
TidExprListCreate(TidRangeScanState *tidrangestate)
```
## Detailed Description
This function processes the tidrangequals list from a TidRangeScan plan node, converting each OpExpr into a corresponding TidOpExpr structure. It iterates through all qualification expressions that involve CTID comparisons, validates that each expression is indeed an OpExpr, and uses MakeTidOpExpr to create the appropriate TidOpExpr structure. The resulting list of TidOpExpr structures is stored in the TidRangeScanState for use during scan execution.

## Parameters / Member Variables
- `tidrangestate`: TidRangeScanState structure that will store the processed TID expressions and provides access to the plan node

## Dependencies
- Functions called/Symbols referenced:
  - lfirst
  - IsA
  - elog
  - [MakeTidOpExpr](../M/MakeTidOpExpr.md)
  - [lappend](../l/lappend.md)
- Data structures used:
  - [TidRangeScan](TidRangeScan.md)
  - [List](../L/List.md)
  - ListCell
  - [OpExpr](../O/OpExpr.md)
  - [TidOpExpr](TidOpExpr.md)
- Called from:
  - [ExecInitTidRangeScan](../E/ExecInitTidRangeScan.md)
  - [ExecInitTidScan](../E/ExecInitTidScan.md)

## Notes and Other Information
- The function validates that all expressions in tidrangequals are OpExpr nodes
- Throws an error if any non-OpExpr expression is encountered
- The processed expressions are stored in tidrangestate->trss_tidexprs for later use
- This is a preparation step that occurs during executor initialization
- The function transforms plan-time expressions into execution-time structures

## Simplified Source

```c
static void
TidExprListCreate(TidRangeScanState *tidrangestate)
{
    TidRangeScan *node = (TidRangeScan *) tidrangestate->ss.ps.plan;
    List *tidexprs = NIL;
    ListCell *l;

    // Process each TID qualification expression from the plan
    foreach(l, node->tidrangequals) {
        OpExpr *opexpr = lfirst(l);
        TidOpExpr *tidopexpr;

        // Validate that expression is an OpExpr (CTID comparison)
        if (!IsA(opexpr, OpExpr))
            elog(ERROR, "could not identify CTID expression");

        // Convert OpExpr to TidOpExpr for execution
        tidopexpr = MakeTidOpExpr(opexpr, tidrangestate);
        tidexprs = lappend(tidexprs, tidopexpr);
    }

    // Store processed expressions for scan execution
    tidrangestate->trss_tidexprs = tidexprs;
}
```