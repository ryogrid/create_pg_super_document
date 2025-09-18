# TidExprListCreate

## Location
src/backend/executor/nodeTidrangescan.c: 106 - 136

## Overview
TidExprListCreate extracts and processes TID qualification expressions from a TidRangeScan plan node, converting them into a list of TidOpExpr structures for efficient TID range scanning.

## Definition


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
  - lappend
- Data structures used:
  - TidRangeScan
  - [List](../L/List.md)
  - ListCell
  - OpExpr
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