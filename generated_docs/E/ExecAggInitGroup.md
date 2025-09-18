# ExecAggInitGroup

## Location
src/backend/executor/execExprInterp.c: 5017 - 5069

## Overview
ExecAggInitGroup initializes the aggregation group state when processing the first non-NULL input value for an aggregate function.

## Definition
void ExecAggInitGroup(AggState *aggstate, AggStatePerTrans pertrans, AggStatePerGroup pergroup, ExprContext *aggcontext)

## Detailed Description
This function is responsible for initializing an aggregate group when the first non-NULL input value is encountered. It copies the input datum into the appropriate memory context and sets up the transition value for the aggregate computation. The function ensures that pass-by-reference values are properly copied into the aggregation context memory to avoid memory management issues. It marks the group as having a valid transition value and clears the noTransValue flag.

## Parameters / Member Variables
- aggstate: AggState pointer containing the overall aggregation state
- pertrans: AggStatePerTrans pointer containing per-transition function state information
- pergroup: AggStatePerGroup pointer containing per-group aggregation state to be initialized
- aggcontext: ExprContext pointer providing the aggregation memory context

## Dependencies
- Functions called/Symbols referenced:
  - [datumCopy](../d/datumCopy.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
- Called from (representative examples):
  - [ExecInterpExpr](ExecInterpExpr.md)
  - [FunctionReturningBool](../F/FunctionReturningBool.md) (in JIT compilation context)

## Notes and Other Information
- This function assumes the aggregate input type is binary-compatible with its transition type
- Memory management is carefully handled by switching to the per-tuple memory context before copying data
- The function sets transValueIsNull to false and noTransValue to false to indicate a valid transition value
- Part of PostgreSQL aggregation execution framework for handling group-based aggregates
- Located in src/backend/executor/execExprInterp.c at lines 5017-5069