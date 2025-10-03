# build_pertrans_for_aggref

## Location
[src/backend/executor/nodeAgg.c:4038-4287](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L4038-L4287)

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
  - [build_aggregate_transfn_expr](build_aggregate_transfn_expr.md), build_aggregate_serialfn_expr, build_aggregate_deserialfn_expr
  - [fmgr_info](../f/fmgr_info.md), fmgr_info_set_expr
  - InitFunctionCallInfoData, SizeForFunctionCallInfo
  - [get_typlenbyval](../g/get_typlenbyval.md), get_opcode, get_sortgroupclause_tle
  - [ExecTypeFromTL](../E/ExecTypeFromTL.md), ExecInitExtraTupleSlot
  - [execTuplesMatchPrepare](../e/execTuplesMatchPrepare.md), exprCollation
- Types used:
  - [AggStatePerTrans](../A/AggStatePerTrans.md), AggState, EState, Aggref
  - [FunctionCallInfo](../F/FunctionCallInfo.md), SortGroupClause, TargetEntry, Tuplesortstate
- Constants used:
  - AGGKIND_IS_ORDERED_SET, DO_AGGSPLIT_COMBINE
  - AGG_HASHED, AGG_MIXED, TTSOpsMinimalTuple
- Called from:
  - [ExecInitAgg](../E/ExecInitAgg.md) (src/backend/executor/nodeAgg.c:3887, 3913)

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

## Simplified Source

```c
static void build_pertrans_for_aggref(AggStatePerTrans pertrans,
                                     AggState *aggstate, EState *estate,
                                     Aggref *aggref,
                                     Oid transfn_oid, Oid aggtranstype,
                                     Oid aggserialfn, Oid aggdeserialfn,
                                     Datum initValue, bool initValueIsNull,
                                     Oid *inputTypes, int numArguments) {

    // Initialize basic aggregate state fields
    pertrans->aggref = aggref;
    pertrans->aggshared = false;
    pertrans->aggCollation = aggref->inputcollid;
    pertrans->transfn_oid = transfn_oid;
    pertrans->initValue = initValue;
    pertrans->initValueIsNull = initValueIsNull;
    pertrans->aggtranstype = aggtranstype;

    int numDirectArgs = list_length(aggref->aggdirectargs);
    pertrans->numInputs = list_length(aggref->args);
    int numTransArgs = pertrans->numTransInputs + 1;

    // Set up transition function call infrastructure
    Expr *transfnexpr;
    build_aggregate_transfn_expr(inputTypes, numArguments, numDirectArgs,
                                aggref->aggvariadic, aggtranstype,
                                aggref->inputcollid, transfn_oid, InvalidOid,
                                &transfnexpr, NULL);

    fmgr_info(transfn_oid, &pertrans->transfn);
    fmgr_info_set_expr((Node *) transfnexpr, &pertrans->transfn);

    // Allocate and initialize function call info
    pertrans->transfn_fcinfo =
        (FunctionCallInfo) palloc(SizeForFunctionCallInfo(numTransArgs));
    InitFunctionCallInfoData(*pertrans->transfn_fcinfo, &pertrans->transfn,
                            numTransArgs, pertrans->aggCollation,
                            (void *) aggstate, NULL);

    // Get state value datatype info
    get_typlenbyval(aggtranstype, &pertrans->transtypeLen, &pertrans->transtypeByVal);

    // Set up serialization functions (if needed for parallel aggregation)
    if (OidIsValid(aggserialfn)) {
        // Setup serialization function call infrastructure
        Expr *serialfnexpr;
        build_aggregate_serialfn_expr(aggserialfn, &serialfnexpr);
        fmgr_info(aggserialfn, &pertrans->serialfn);
        // ... function call info setup
    }

    if (OidIsValid(aggdeserialfn)) {
        // Setup deserialization function call infrastructure
        Expr *deserialfnexpr;
        build_aggregate_deserialfn_expr(aggdeserialfn, &deserialfnexpr);
        fmgr_info(aggdeserialfn, &pertrans->deserialfn);
        // ... function call info setup
    }

    // Determine sorting requirements based on DISTINCT/ORDER BY clauses
    List *sortlist;
    int numSortCols, numDistinctCols;

    if (AGGKIND_IS_ORDERED_SET(aggref->aggkind)) {
        // Ordered-set aggregates handle their own sorting
        sortlist = NIL;
        numSortCols = numDistinctCols = 0;
        pertrans->aggsortrequired = false;
    } else if (aggref->aggdistinct) {
        // DISTINCT clause requires sorting
        sortlist = aggref->aggdistinct;
        numSortCols = numDistinctCols = list_length(sortlist);
        pertrans->aggsortrequired = !aggref->aggpresorted;
    } else {
        // Handle ORDER BY clause
        sortlist = aggref->aggorder;
        numSortCols = list_length(sortlist);
        numDistinctCols = 0;
        pertrans->aggsortrequired = (numSortCols > 0);
    }

    pertrans->numSortCols = numSortCols;
    pertrans->numDistinctCols = numDistinctCols;

    // Set up sorting infrastructure if needed
    if (numSortCols > 0 || aggref->aggfilter) {
        pertrans->sortdesc = ExecTypeFromTL(aggref->args);
        pertrans->sortslot = ExecInitExtraTupleSlot(estate, pertrans->sortdesc,
                                                   &TTSOpsMinimalTuple);
    }

    // Configure sorting details (column indexes, operators, collations)
    if (numSortCols > 0) {
        // Allocate arrays for sort configuration
        pertrans->sortColIdx = (AttrNumber *) palloc(numSortCols * sizeof(AttrNumber));
        pertrans->sortOperators = (Oid *) palloc(numSortCols * sizeof(Oid));
        pertrans->sortCollations = (Oid *) palloc(numSortCols * sizeof(Oid));
        pertrans->sortNullsFirst = (bool *) palloc(numSortCols * sizeof(bool));

        // Extract sort information from sort clauses
        int i = 0;
        ListCell *lc;
        foreach(lc, sortlist) {
            SortGroupClause *sortcl = (SortGroupClause *) lfirst(lc);
            TargetEntry *tle = get_sortgroupclause_tle(sortcl, aggref->args);

            pertrans->sortColIdx[i] = tle->resno;
            pertrans->sortOperators[i] = sortcl->sortop;
            pertrans->sortCollations[i] = exprCollation((Node *) tle->expr);
            pertrans->sortNullsFirst[i] = sortcl->nulls_first;
            i++;
        }
    }

    // Set up DISTINCT comparison functions
    if (aggref->aggdistinct) {
        Oid *equality_ops = palloc(numDistinctCols * sizeof(Oid));

        // Extract equality operators from DISTINCT clauses
        int i = 0;
        ListCell *lc;
        foreach(lc, aggref->aggdistinct) {
            equality_ops[i++] = ((SortGroupClause *) lfirst(lc))->eqop;
        }

        // Set up appropriate comparison strategy
        if (numDistinctCols == 1) {
            fmgr_info(get_opcode(equality_ops[0]), &pertrans->equalfnOne);
        } else {
            pertrans->equalfnMulti = execTuplesMatchPrepare(pertrans->sortdesc,
                                                           numDistinctCols,
                                                           pertrans->sortColIdx,
                                                           equality_ops,
                                                           pertrans->sortCollations,
                                                           &aggstate->ss.ps);
        }
        pfree(equality_ops);
    }

    // Initialize sort state array for grouping sets
    int numGroupingSets = Max(aggstate->maxsets, 1);
    pertrans->sortstates = (Tuplesortstate **)
        palloc0(sizeof(Tuplesortstate *) * numGroupingSets);
}
```