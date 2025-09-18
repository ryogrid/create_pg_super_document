# build_pertrans_for_aggref

## Location
src/backend/executor/nodeAgg.c: 4038 - 4287

## Overview
Builds and initializes the per-transition state structure for a specific aggregate function, setting up function call infrastructure, sorting requirements, and DISTINCT handling.

## Definition
```c
static void build_pertrans_for_aggref(AggStatePerTrans pertrans,
                                     AggState *aggstate, EState *estate,
                                     Aggref *aggref,
                                     Oid transfn_oid, Oid aggtranstype,
                                     Oid aggserialfn, Oid aggdeserialfn,
                                     Datum initValue, bool initValueIsNull,
                                     Oid *inputTypes, int numArguments)
```

## Detailed Description
This function performs comprehensive initialization of an AggStatePerTrans structure for a specific aggregate function. It handles the complex setup required for aggregate transition function calls, including:

**Core transition function setup:**
- Builds expression trees for transition, serialization, and deserialization functions
- Sets up FunctionCallInfo structures with proper argument counts and collation
- Configures function manager information for efficient function calls

**Sorting and DISTINCT handling:**
- Determines sorting requirements based on ORDER BY and DISTINCT clauses
- Creates tuple descriptors and slots for sorted aggregate inputs
- Sets up comparison functions for DISTINCT value detection
- Handles presorted input optimization when planner indicates input is already sorted

**Special aggregate types:**
- Skips sorting setup for ordered-set aggregates (handled by aggregate functions themselves)
- Creates additional slots for multi-column DISTINCT operations
- Configures single-column vs multi-column DISTINCT comparison strategies

The function carefully handles various aggregate scenarios including partial aggregation, parallel aggregation with serialization/deserialization, and different sorting strategies.

## Parameters / Member Variables
- `pertrans`: The per-transition state structure to initialize
- `aggstate`: The aggregate execution state providing context
- `estate`: The execution state for memory management and slot creation
- `aggref`: The aggregate reference containing configuration from the parser
- `transfn_oid`: OID of the transition function (or combine function for partial aggregation)
- `aggtranstype`: Data type of the aggregate's transition state
- `aggserialfn`: OID of serialization function (for parallel aggregation)
- `aggdeserialfn`: OID of deserialization function (for parallel aggregation)
- `initValue`: Initial value for the transition state
- `initValueIsNull`: Whether the initial value is NULL
- `inputTypes`: Array of input argument types
- `numArguments`: Number of input arguments

## Dependencies
- Functions called/Symbols referenced:
  - build_aggregate_transfn_expr, build_aggregate_serialfn_expr, build_aggregate_deserialfn_expr
  - fmgr_info, fmgr_info_set_expr
  - InitFunctionCallInfoData, SizeForFunctionCallInfo
  - get_typlenbyval, get_opcode, get_sortgroupclause_tle
  - ExecTypeFromTL, ExecInitExtraTupleSlot
  - execTuplesMatchPrepare, exprCollation
- Types used:
  - AggStatePerTrans, AggState, EState, Aggref
  - FunctionCallInfo, SortGroupClause, TargetEntry, Tuplesortstate
- Constants used:
  - AGGKIND_IS_ORDERED_SET, DO_AGGSPLIT_COMBINE
  - AGG_HASHED, AGG_MIXED, TTSOpsMinimalTuple
- Called from:
  - ExecInitAgg (src/backend/executor/nodeAgg.c:3887, 3913)

## Notes and Other Information
- This is a static function internal to nodeAgg.c
- Handles both regular transition functions and combine functions for partial aggregation
- DISTINCT and ORDER BY aggregates are not supported with hashed aggregation strategies
- ORDER BY aggregates are not supported with partial aggregation (AGGSPLIT_COMBINE)
- Creates separate tuple slots for sorting operations and DISTINCT value tracking
- For single-column DISTINCT, uses optimized single-function comparison
- For multi-column DISTINCT, uses prepared tuple comparison expressions
- Allocates Tuplesortstate arrays sized for the maximum number of grouping sets
- Serialization/deserialization functions are only set up when valid OIDs are provided
- The function carefully handles presorted input optimization for improved performance