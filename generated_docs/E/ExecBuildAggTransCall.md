# ExecBuildAggTransCall

## Location
[src/backend/executor/execExpr.c:3840-3956](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExpr.c#L3840-L3956)

## Overview
Builds transition/combine function invocation for a single transition value in PostgreSQL's aggregate execution. This function is separated from ExecBuildAggTrans() to support multiple callsites (hash and sort in grouping set cases).

## Definition

```c
static void
ExecBuildAggTransCall(ExprState *state, AggState *aggstate,
					  ExprEvalStep *scratch,
					  FunctionCallInfo fcinfo, AggStatePerTrans pertrans,
					  int transno, int setno, int setoff, bool ishash,
					  bool nullcheck)
```
## Detailed Description
ExecBuildAggTransCall constructs the appropriate expression evaluation steps for executing aggregate transition functions. The function intelligently selects different execution opcodes based on the characteristics of the aggregate function:

- For non-ordered aggregates and ORDER BY/DISTINCT aggregates with presorted input, it chooses between strict and non-strict variants
- For ordered aggregates, it selects between single-column and multi-column processing paths
- Handles both by-value and by-reference transition state types
- Supports null checking when required
- Optimizes performance by using specialized opcodes for different scenarios

The function determines the execution context (hash or regular aggregate context) and builds the appropriate evaluation steps, including optional null checks and proper jump target fixups.

## Parameters / Member Variables
- `*state`: ExprState structure containing the expression evaluation steps being built
- `*aggstate`: AggState structure containing aggregate execution state information
- `*scratch`: ExprEvalStep structure used as a template for building new evaluation steps
- `fcinfo`: FunctionCallInfo structure containing function call metadata
- `pertrans`: AggStatePerTrans structure containing per-transition state information
- `transno`: Integer identifying the transition number
- `setno`: Integer identifying the grouping set number
- `setoff`: Integer offset within the grouping set
- `ishash`: Boolean indicating whether this is for hash aggregation
- `nullcheck`: Boolean indicating whether null checking is required
## Dependencies
- Functions called/Symbols referenced:
  - [ExprEvalPushStep](ExprEvalPushStep.md)
  - [AggState](../A/AggState.md)
  - [ExprEvalStep](ExprEvalStep.md)
  - [FunctionCallInfo](../F/FunctionCallInfo.md)
  - [AggStatePerTrans](../A/AggStatePerTrans.md)
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
  - [ExecBuildAggTrans](ExecBuildAggTrans.md)

## Notes and Other Information
- This is a static function in src/backend/executor/execExpr.c (lines 3840-3956)
- The function implements performance-critical optimizations for aggregate execution by selecting appropriate opcodes
- Handles complex logic for determining when to use strict vs non-strict function calls
- Supports both hash-based and sort-based aggregation strategies
- The opcode selection logic is designed to minimize runtime checks during aggregate execution
- Jump target fixups ensure proper control flow for null checking scenarios

## Simplified Source

```c
static void
ExecBuildAggTransCall(ExprState *state, AggState *aggstate,
                      ExprEvalStep *scratch,
                      FunctionCallInfo fcinfo, AggStatePerTrans pertrans,
                      int transno, int setno, int setoff, bool ishash,
                      bool nullcheck)
{
    // Determine execution context (hash vs regular)
    ExprContext *aggcontext;
    if (ishash)
        aggcontext = aggstate->hashcontext;
    else
        aggcontext = aggstate->aggcontexts[setno];

    // Add null check step if required
    int adjust_jumpnull = -1;
    if (nullcheck)
    {
        scratch->opcode = EEOP_AGG_PLAIN_PERGROUP_NULLCHECK;
        scratch->d.agg_plain_pergroup_nullcheck.setoff = setoff;
        scratch->d.agg_plain_pergroup_nullcheck.jumpnull = -1; // Fixed later
        ExprEvalPushStep(state, scratch);
        adjust_jumpnull = state->steps_len - 1;
    }

    // Select appropriate transition function opcode based on characteristics
    if (!pertrans->aggsortrequired)
    {
        // Non-ordered aggregates: choose based on strictness and data type
        if (pertrans->transtypeByVal)
        {
            if (fcinfo->flinfo->fn_strict && pertrans->initValueIsNull)
                scratch->opcode = EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL;
            else if (fcinfo->flinfo->fn_strict)
                scratch->opcode = EEOP_AGG_PLAIN_TRANS_STRICT_BYVAL;
            else
                scratch->opcode = EEOP_AGG_PLAIN_TRANS_BYVAL;
        }
        else
        {
            if (fcinfo->flinfo->fn_strict && pertrans->initValueIsNull)
                scratch->opcode = EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYREF;
            else if (fcinfo->flinfo->fn_strict)
                scratch->opcode = EEOP_AGG_PLAIN_TRANS_STRICT_BYREF;
            else
                scratch->opcode = EEOP_AGG_PLAIN_TRANS_BYREF;
        }
    }
    else
    {
        // Ordered aggregates: choose based on number of inputs
        if (pertrans->numInputs == 1)
            scratch->opcode = EEOP_AGG_ORDERED_TRANS_DATUM;
        else
            scratch->opcode = EEOP_AGG_ORDERED_TRANS_TUPLE;
    }

    // Setup step data and add to expression
    scratch->d.agg_trans.pertrans = pertrans;
    scratch->d.agg_trans.setno = setno;
    scratch->d.agg_trans.setoff = setoff;
    scratch->d.agg_trans.transno = transno;
    scratch->d.agg_trans.aggcontext = aggcontext;
    ExprEvalPushStep(state, scratch);

    // Fix up null check jump target if needed
    if (adjust_jumpnull != -1)
    {
        ExprEvalStep *nullcheck_step = &state->steps[adjust_jumpnull];
        nullcheck_step->d.agg_plain_pergroup_nullcheck.jumpnull = state->steps_len;
    }
}
```