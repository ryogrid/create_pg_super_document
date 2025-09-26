# initialize_peragg

## Location
[src/backend/executor/nodeWindowAgg.c:2748-3020](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeWindowAgg.c#L2748-L3020)

## Overview
initialize_peragg initializes per-aggregate execution state for window functions by setting up transition and final functions, memory contexts, and determining whether to use moving aggregates optimization.

## Definition
```c
static WindowStatePerAggData *initialize_peragg(WindowAggState *winstate, WindowFunc *wfunc, WindowStatePerAgg peraggstate)
```

## Detailed Description
initialize_peragg is a comprehensive initialization function for window aggregates that performs several critical tasks: 1) Analyzes the aggregate function definition to determine if moving aggregates can be safely used based on frame options, function volatility, and safety constraints, 2) Sets up appropriate transition and inverse transition functions, final functions, and their expression trees, 3) Performs permission checks to ensure the aggregate owner can execute all component functions, 4) Validates that the aggregate is suitable for window function usage (e.g., final function must be read-only), 5) Resolves polymorphic types and builds function call expressions, 6) Creates dedicated memory contexts for moving aggregates to handle their different lifecycle requirements, and 7) Initializes the aggregate state with proper initial values. The function carefully balances performance optimization through moving aggregates against correctness and safety requirements.

## Parameters / Member Variables
- `winstate`: WindowAggState containing the overall window aggregation execution state
- `wfunc`: WindowFunc structure describing the window function to initialize
- `peraggstate`: WindowStatePerAgg structure to be initialized with aggregate-specific state

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md), ReleaseSysCache
  - [contain_volatile_functions](../c/contain_volatile_functions.md), contain_subplans
  - [object_aclcheck](../o/object_aclcheck.md), aclcheck_error
  - [resolve_aggregate_transtype](../r/resolve_aggregate_transtype.md)
  - [build_aggregate_transfn_expr](../b/build_aggregate_transfn_expr.md), build_aggregate_finalfn_expr
  - [fmgr_info](../f/fmgr_info.md), fmgr_info_set_expr
  - [get_typlenbyval](../g/get_typlenbyval.md)
  - [GetAggInitVal](../G/GetAggInitVal.md)
  - AllocSetContextCreate
- Called from (representative examples):
  - [ExecInitWindowAgg](../E/ExecInitWindowAgg.md) (during window aggregate node initialization)

## Notes and Other Information
- Almost identical to nodeAgg.c implementation except DISTINCT is not supported for window functions
- Moving aggregates decision logic considers safety, frame options, volatility, and subplan presence
- Creates separate memory contexts for moving aggregates to handle different restart patterns
- Validates strictness compatibility between forward and inverse transition functions
- Performs extensive permission checking for all aggregate component functions
- Located in src/backend/executor/nodeWindowAgg.c:2748-3020