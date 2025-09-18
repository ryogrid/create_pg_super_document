# ExecInitExprList

## Location
src/backend/executor/execExpr.c: 327 - 361

## Overview
ExecInitExprList is a utility function that applies ExecInitExpr to each expression in a list, returning a corresponding list of compiled ExprStates.

## Definition


## Detailed Description
ExecInitExprList is a straightforward utility function that provides batch compilation of expression lists. It iterates through a list of Expr nodes and calls ExecInitExpr on each one, collecting the resulting ExprState pointers into a new list that maintains the same ordering as the input.

This function is commonly used throughout the executor when multiple expressions need to be compiled together, such as:
- Target lists for projections
- Multiple expressions in aggregate functions
- Lists of expressions in table functions
- Hash key expressions in hash joins
- Multiple expressions in VALUES clauses

The function maintains a one-to-one correspondence between input expressions and output ExprStates, making it easy for callers to correlate compiled expressions with their original sources.

## Parameters / Member Variables
- : A List of Expr nodes to be compiled. Each element should be a valid expression node.
- : The PlanState node that will own all the compiled expressions. This is passed through to each ExecInitExpr call.

## Dependencies
- Functions called/Symbols referenced:
  - [ExecInitExpr](ExecInitExpr.md) (compiles each individual expression)
  - foreach (macro for list iteration)
  - lfirst (extracts list cell content)
  - lappend (builds result list)
- Called from (representative examples):
  - [ExecInitAgg](ExecInitAgg.md) (for aggregate target lists)
  - [ExecInitHash](ExecInitHash.md) (for hash key expressions)
  - [ExecInitHashJoin](ExecInitHashJoin.md) (for hash expressions)
  - [ExecInitTableFuncScan](ExecInitTableFuncScan.md) (for table function expressions)
  - [ExecInitValuesScan](ExecInitValuesScan.md) (for VALUES expressions)
  - [ExecInitExprRec](ExecInitExprRec.md) (for recursive expression compilation)

## Notes and Other Information
- Returns NIL (empty list) when given an empty input list
- Preserves the order of expressions from input to output
- Each ExprState in the result list can be independently evaluated with ExecEvalExpr
- Memory allocation for the result list occurs in the current memory context
- Does not perform any optimization or deduplication - each input expression gets its own ExprState
- Widely used utility function throughout the executor for any scenario requiring multiple expression compilation
- The resulting ExprStates share the same parent PlanState but are otherwise independent