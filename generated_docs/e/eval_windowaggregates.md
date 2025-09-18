# eval_windowaggregates

## Location
[src/backend/executor/nodeWindowAgg.c:663-1032](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeWindowAgg.c#L663-L1032)

## Overview
Evaluates plain aggregates being used as window functions, managing frame boundaries and optimizing computation through incremental updates and inverse transitions.

## Definition


## Detailed Description
This is the core function for evaluating window aggregates and differs significantly from nodeAgg.c in two key ways: it uses inverse transition functions to remove rows when the window frame start moves, and it supports calling aggregate final functions repeatedly on the same transition value. The function implements sophisticated optimizations including incremental aggregation for contiguous frames, frame reuse when successive rows share identical frames, and selective restart strategies. It handles complex frame semantics including exclusion clauses, manages memory contexts carefully, and coordinates between forward aggregation (via ) and backward removal (via ).

## Parameters / Member Variables
- : The complete window aggregate execution state containing all per-function and per-aggregate states, frame positions, memory contexts, and optimization flags

## Dependencies
- Functions called/Symbols referenced:
  - [update_frameheadpos](../u/update_frameheadpos.md)
  - [window_gettupleslot](../w/window_gettupleslot.md)
  - [advance_windowaggregate_base](../a/advance_windowaggregate_base.md)
  - ResetExprContext
  - ExecClearTuple
  - [WinSetMarkPosition](../W/WinSetMarkPosition.md)
  - [MemoryContextReset](../M/MemoryContextReset.md)
  - [initialize_windowaggregate](../i/initialize_windowaggregate.md)
  - TupIsNull
  - [row_is_in_frame](../r/row_is_in_frame.md)
  - [advance_windowaggregate](../a/advance_windowaggregate.md)
  - [finalize_windowaggregate](../f/finalize_windowaggregate.md)
  - [datumCopy](../d/datumCopy.md)
- Called from (representative examples):
  - [ExecWindowAgg](../E/ExecWindowAgg.md)

## Notes and Other Information
- Implements multiple optimization strategies: incremental aggregation for UNBOUNDED_PRECEDING frames, inverse transitions for moving frames, and frame result reuse for identical frames
- Handles restart conditions: first row in partition, frame head movement without inverse functions, exclusion clauses, or non-overlapping frames
- Manages  and  pointers to track which rows have been processed and which need processing
- For moving frames, attempts to use inverse transition functions to remove rows that fall out of the frame, falling back to full restart if inverse transitions fail
- Supports shared and private aggregate memory contexts with different cleanup strategies
- Maintains loop invariant that  is either empty or contains the row at 
- Saves aggregate results in per-aggregate state to enable frame result reuse for subsequent rows with identical frames
- Handles exclusion clauses by punting to full recalculation for every row (optimization opportunity for contiguous exclusions)
- Frame end position is validated to never move backwards to ensure correctness