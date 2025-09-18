# advance_transition_function

## Location
src/backend/executor/nodeAgg.c: 706 - 815

## Overview
Advances the transition function for a single aggregate state within one grouping set, processing new input values and updating the aggregate's transValue.

## Definition
```c
static void advance_transition_function(AggState *aggstate,
                                      AggStatePerTrans pertrans,
                                      AggStatePerGroup pergroupstate)
```

## Detailed Description
This function is responsible for advancing the transition function of an aggregate during query execution. It handles the core logic of aggregate computation by taking new input values and applying the aggregate's transition function to update the current transValue. The function includes sophisticated memory management, handling both strict and non-strict transition functions, and manages the initialization of transValue for the first non-NULL input.

Key behaviors include:
- For strict transition functions, skips processing when any input is NULL
- Handles first-time initialization of transValue using the first non-NULL input
- Manages memory contexts appropriately to prevent memory leaks
- Optimizes pass-by-reference datatypes by avoiding unnecessary copying when the transition function returns its input argument unchanged

## Parameters / Member Variables
- `aggstate`: The main aggregate state containing context information and temporary memory contexts
- `pertrans`: Per-transition state containing the transition function info, type information, and function call context
- `pergroupstate`: Per-group state containing the current transValue and associated flags for this specific grouping

## Dependencies
- Functions called/Symbols referenced:
  - datumCopy
  - FunctionCallInvoke
  - ExecAggCopyTransValue
- Data types used:
  - AggState
  - AggStatePerTrans
  - AggStatePerGroup
  - FunctionCallInfo
- Called from (representative examples):
  - process_ordered_aggregate_single
  - process_ordered_aggregate_multi

## Notes and Other Information
- The function operates within the assumption that input values are already preloaded in pertrans->transfn_fcinfo
- Memory context switching ensures proper allocation and cleanup of temporary values
- The function handles the complex case where transition functions may return pointers to their input arguments to avoid unnecessary memory copying
- Static fields of the fcinfo are expected to be initialized by ExecInitAgg() before calling this function
- Designed to work regardless of the calling memory context