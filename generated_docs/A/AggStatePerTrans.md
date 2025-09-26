# AggStatePerTrans

## Location
[src/include/nodes/execnodes.h:2458-2458](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L2458-L2458)

## Overview
AggStatePerTrans is a typedef pointer to AggStatePerTransData structure that represents per-transition-state information for aggregate functions, managing the working state and transition function execution during aggregate computation.

## Definition
```c
typedef struct AggStatePerTransData *AggStatePerTrans;
```

Where AggStatePerTransData is a comprehensive structure containing transition state management information, including function lookup data, sorting and distinctness handling, and working state for aggregate computation.

## Detailed Description
AggStatePerTrans manages the transition state for aggregate functions during execution. Unlike AggStatePerAgg which handles final result production, this structure focuses on maintaining and updating the intermediate state values as input rows are processed. It handles complex scenarios including DISTINCT processing, ORDER BY requirements, and state sharing between multiple identical aggregates. The structure contains both static configuration (set during ExecInitAgg) and dynamic working state that changes as input tuples are processed. It supports various aggregate types including simple aggregates, ordered-set aggregates, and those requiring DISTINCT processing through integrated sorting mechanisms.

## Parameters / Member Variables
- `aggref`: Pointer to the first Aggref expression using this transition state
- `aggshared`: Whether this state is shared by multiple Aggref expressions
- `aggsortrequired`: True for ORDER BY and DISTINCT aggregates that aren't pre-sorted
- `numInputs`: Number of aggregated input columns including ORDER BY expressions
- `numTransInputs`: Number of columns to pass to the transition function
- `transfn_oid`: OID of the state transition or combine function
- `serialfn_oid`: OID of the serialization function (or InvalidOid)
- `deserialfn_oid`: OID of the deserialization function (or InvalidOid)
- `aggtranstype`: OID of the state value's data type
- `transfn`: Function manager lookup data for transition/combine function
- `serialfn`: Function manager lookup data for serialization function
- `deserialfn`: Function manager lookup data for deserialization function
- `aggCollation`: Input collation derived for the aggregate
- `numSortCols`: Number of sorting columns
- `numDistinctCols`: Number of columns for DISTINCT comparisons
- `sortColIdx`: Array of sort column indices
- `sortOperators`: Array of sort operator OIDs
- `sortCollations`: Array of sort collation OIDs
- `sortNullsFirst`: Array of null-ordering flags
- `equalfnOne`: Comparator for single-column DISTINCT
- `equalfnMulti`: Expression state for multi-column DISTINCT comparisons
- `initValue`: Initial aggregate value from pg_aggregate
- `initValueIsNull`: Whether the initial value is NULL
- `inputtypeLen`: Length of input data type
- `transtypeLen`: Length of transition state data type
- `inputtypeByVal`: Whether input type is passed by value
- `transtypeByVal`: Whether transition type is passed by value
- `sortslot`: Tuple slot for current input tuple
- `uniqslot`: Tuple slot for multi-column DISTINCT processing
- `sortdesc`: Tuple descriptor for input tuples
- `lastdatum`: Last value for single-column DISTINCT checking
- `lastisnull`: Whether last value was NULL
- `haslast`: Whether we have a last value for DISTINCT
- `sortstates`: Array of tuplesort objects for DISTINCT/ORDER BY
- `transfn_fcinfo`: Pre-initialized function call info for transition function
- `serialfn_fcinfo`: Pre-initialized function call info for serialization
- `deserialfn_fcinfo`: Pre-initialized function call info for deserialization

## Dependencies
- Functions called/Symbols referenced:
  - [AggStatePerTransData](AggStatePerTransData.md)
  - [Aggref](Aggref.md)
  - [FmgrInfo](../F/FmgrInfo.md)
  - [ExprState](../E/ExprState.md)
  - [TupleTableSlot](../T/TupleTableSlot.md)
  - [TupleDesc](../T/TupleDesc.md)
  - [Tuplesortstate](../T/Tuplesortstate.md)
  - [FunctionCallInfo](../F/FunctionCallInfo.md)
- Called from (representative examples):
  - [ExecBuildAggTrans](../E/ExecBuildAggTrans.md)
  - [ExecInterpExpr](../E/ExecInterpExpr.md) (multiple aggregate evaluation functions)
  - [initialize_aggregate](../i/initialize_aggregate.md)
  - [advance_transition_function](../a/advance_transition_function.md)
  - [process_ordered_aggregate_single](../p/process_ordered_aggregate_single.md)
  - [process_ordered_aggregate_multi](../p/process_ordered_aggregate_multi.md)
  - [finalize_aggregate](../f/finalize_aggregate.md)
  - [ExecInitAgg](../E/ExecInitAgg.md)
  - [build_pertrans_for_aggref](../b/build_pertrans_for_aggref.md)

## Notes and Other Information
- This structure is the core of PostgreSQL's aggregate state management, handling the complex logic for maintaining transition states
- Supports state sharing optimization where multiple identical aggregates can use the same transition state
- Includes comprehensive support for DISTINCT and ORDER BY processing through integrated tuplesort functionality
- Contains pre-initialized function call information to optimize performance during repeated transition function calls
- The separation between transition state (AggStatePerTrans) and final result production (AggStatePerAgg) allows multiple aggregates to share intermediate computation
- Handles both simple aggregates and complex ordered-set aggregates with direct arguments
- Critical for parallel aggregation where serialization/deserialization functions enable state transfer between workers