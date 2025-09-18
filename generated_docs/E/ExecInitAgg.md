# ExecInitAgg

## Location
[src/backend/executor/nodeAgg.c:3173-4037](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L3173-L4037)

## Overview
Initializes the aggregate execution state for PostgreSQL's aggregate node, setting up all necessary data structures, memory contexts, and function lookup information for aggregate computation.

## Definition
```c
AggState *ExecInitAgg(Agg *node, EState *estate, int eflags)
```

## Detailed Description
ExecInitAgg is the comprehensive initialization function for PostgreSQL's aggregate execution engine. It performs extensive setup work to prepare for aggregate computation across potentially multiple phases and grouping sets. The function handles various aggregation strategies (plain, sorted, hashed, mixed) and sets up all required infrastructure:

**Key initialization tasks:**
1. **State structure creation**: Allocates and initializes the main AggState structure with basic configuration
2. **Memory context setup**: Creates specialized expression contexts for per-input processing, per-output processing, hashtables, and each grouping set
3. **Child node initialization**: Initializes the outer plan node that provides input tuples
4. **Slot and projection setup**: Configures tuple slots and projection information for input/output handling  
5. **Phase and grouping set configuration**: Sets up multi-phase processing for complex aggregation strategies and multiple grouping sets
6. **Aggregate function processing**: Performs function lookup, permission checking, and state setup for each aggregate function
7. **Hash table preparation**: For hashed aggregation, sets up hash tables, spill infrastructure, and memory limits
8. **Expression compilation**: Builds optimized transition function expressions for each execution phase

The function handles complex scenarios like partial aggregation, aggregate splitting for parallel processing, serialization/deserialization of aggregate states, and mixed aggregation strategies that combine hashing and sorting.

## Parameters / Member Variables
- `node`: The Agg plan node containing aggregation configuration from the planner
- `estate`: The execution state providing query context and memory management
- `eflags`: Execution flags controlling behavior (e.g., EXEC_FLAG_EXPLAIN_ONLY)

## Dependencies
- Functions called/Symbols referenced:
  - ExecAssignExprContext, ExecInitNode, ExecGetResultSlotOps
  - ExecCreateScanSlotFromOuterPlan, ExecInitExtraTupleSlot
  - [ExecInitResultTupleSlotTL](ExecInitResultTupleSlotTL.md), ExecAssignProjectionInfo
  - [ExecInitQual](ExecInitQual.md), ExecInitExprList
  - [CreateWorkExprContext](../C/CreateWorkExprContext.md), AllocSetContextCreate
  - [hash_agg_entry_size](../h/hash_agg_entry_size.md), hash_agg_set_limits, find_hash_columns, build_hash_tables
  - [initialize_phase](../i/initialize_phase.md), select_current_set
  - [build_pertrans_for_aggref](../b/build_pertrans_for_aggref.md), build_aggregate_finalfn_expr
  - [ExecBuildAggTrans](ExecBuildAggTrans.md)
  - [get_aggregate_argtypes](../g/get_aggregate_argtypes.md), get_typlenbyval
  - [object_aclcheck](../o/object_aclcheck.md), aclcheck_error, InvokeFunctionExecuteHook
- Types used:
  - [AggState](../A/AggState.md), Agg, EState, AggStatePerAgg, AggStatePerTrans, AggStatePerGroup
  - [AggStatePerPhase](../A/AggStatePerPhase.md), AggStatePerHash, Aggref, ExprContext
- Called from:
  - [ExecInitNode](ExecInitNode.md) (src/backend/executor/execProcnode.c:341)

## Notes and Other Information
- This is a complex function with ~860 lines handling multiple aggregation strategies
- Supports AGG_PLAIN, AGG_SORTED, AGG_HASHED, and AGG_MIXED aggregation strategies
- Handles advanced features like partial aggregation, parallel aggregation, and grouping sets
- Performs extensive permission checking for aggregate and transition functions
- Sets up spill-to-disk infrastructure for hash aggregation when memory is limited
- Creates optimized expression trees for transition function evaluation
- Validates that no nested aggregate function calls exist (forbidden by SQL standard)
- Memory contexts are carefully designed for efficient cleanup at group boundaries
- For hashed aggregation, skips hash table allocation during EXPLAIN ONLY operations
- Handles both regular aggregates and ordered-set aggregates with different parameter handling