# ExecInitWholeRowVar

## Location
[src/backend/executor/execExpr.c:2994-3066](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExpr.c#L2994-L3066)

## Overview
Prepares a step for the evaluation of a whole-row variable by initializing the necessary data structures to retrieve complete tuple rows during expression evaluation.

## Definition

```c
static void
ExecInitWholeRowVar(ExprEvalStep *scratch, Var *variable, ExprState *state)
```
## Detailed Description
ExecInitWholeRowVar initializes an expression evaluation step for whole-row variables, which represent entire tuples from a relation rather than individual columns. The function sets up the necessary evaluation context and handles special cases where the input tuple may contain "resjunk" columns (such as GROUP BY or ORDER BY columns) that should be filtered out from the whole-row result.

The function determines if a junk filter is needed by examining the parent plan state. When the parent is a SubqueryScan or CteScan, it checks if the subplan's target list contains any junk columns and creates a JunkFilter if necessary to remove these unwanted columns from the final whole-row result.

## Parameters / Member Variables
- : ExprEvalStep structure to be initialized with whole-row evaluation settings
- : Var node representing the whole-row variable being processed
- : ExprState containing the expression evaluation context and parent plan information

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag (to identify parent plan type)
  - ExecInitJunkFilter (to create junk column filter when needed)
  - [ExecInitExtraTupleSlot](ExecInitExtraTupleSlot.md) (to create tuple slot for filtered results)
- Called from (representative examples):
  - [ExecInitExprRec](ExecInitExprRec.md) (during expression initialization)

## Notes and Other Information
- Sets the opcode to EEOP_WHOLEROW for the evaluation step
- Initializes the wholerow structure with default values (first=true, slow=false, tupdesc=NULL)
- Only creates junk filters for SubqueryScan and CteScan parent nodes
- The tupdesc field is filled at runtime during actual evaluation
- Assumes standalone expressions without parent plans don't need junk filtering