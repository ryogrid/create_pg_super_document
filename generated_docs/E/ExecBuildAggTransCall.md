# ExecBuildAggTransCall

## Location
src/backend/executor/execExpr.c: 3840 - 3956

## Overview
Builds transition/combine function invocation for a single transition value in PostgreSQL's aggregate execution. This function is separated from ExecBuildAggTrans() to support multiple callsites (hash and sort in grouping set cases).

## Definition


## Detailed Description
ExecBuildAggTransCall constructs the appropriate expression evaluation steps for executing aggregate transition functions. The function intelligently selects different execution opcodes based on the characteristics of the aggregate function:

- For non-ordered aggregates and ORDER BY/DISTINCT aggregates with presorted input, it chooses between strict and non-strict variants
- For ordered aggregates, it selects between single-column and multi-column processing paths
- Handles both by-value and by-reference transition state types
- Supports null checking when required
- Optimizes performance by using specialized opcodes for different scenarios

The function determines the execution context (hash or regular aggregate context) and builds the appropriate evaluation steps, including optional null checks and proper jump target fixups.

## Parameters / Member Variables
- : ExprState structure containing the expression evaluation steps being built
- : AggState structure containing aggregate execution state information
- : ExprEvalStep structure used as a template for building new evaluation steps
- : FunctionCallInfo structure containing function call metadata
- : AggStatePerTrans structure containing per-transition state information
- : Integer identifying the transition number
- : Integer identifying the grouping set number
- : Integer offset within the grouping set
- : Boolean indicating whether this is for hash aggregation
- : Boolean indicating whether null checking is required

## Dependencies
- Functions called/Symbols referenced:
  - ExprEvalPushStep
  - AggState
  - ExprEvalStep
  - FunctionCallInfo
  - AggStatePerTrans
  - EEOP_AGG_PLAIN_PERGROUP_NULLCHECK
  - EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL
  - EEOP_AGG_PLAIN_TRANS_STRICT_BYVAL
  - EEOP_AGG_PLAIN_TRANS_BYVAL
  - EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYREF
  - EEOP_AGG_PLAIN_TRANS_STRICT_BYREF
  - EEOP_AGG_PLAIN_TRANS_BYREF
  - EEOP_AGG_ORDERED_TRANS_DATUM
  - EEOP_AGG_ORDERED_TRANS_TUPLE
- Called from (representative examples):
  - ExecBuildAggTrans

## Notes and Other Information
- This is a static function in src/backend/executor/execExpr.c (lines 3840-3956)
- The function implements performance-critical optimizations for aggregate execution by selecting appropriate opcodes
- Handles complex logic for determining when to use strict vs non-strict function calls
- Supports both hash-based and sort-based aggregation strategies
- The opcode selection logic is designed to minimize runtime checks during aggregate execution
- Jump target fixups ensure proper control flow for null checking scenarios