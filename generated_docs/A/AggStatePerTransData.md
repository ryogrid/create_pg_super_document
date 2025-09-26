# AggStatePerTransData

## Location
src/include/executor/nodeAgg.h: 30 - 176

## Overview
AggStatePerTransData represents per-aggregate transition state value information, containing working state for updating an aggregate's state value by calling the transition function with input rows.

## Definition


## Detailed Description
AggStatePerTransData stores the working state needed to update an aggregate's transition state value. This struct is specifically designed for the transition phase of aggregate computation and does not contain information needed to produce the final result (which is stored in AggStatePerAggData). This separation enables multiple aggregate results to be produced from a single transition state.

The struct supports various aggregate types including simple aggregates, DISTINCT aggregates, and ORDER BY aggregates. For simple aggregates, input values are fed directly to the transition function. For DISTINCT or ORDER BY aggregates, input values are processed through a Tuplesort object, with duplicates eliminated and values sorted before applying the transition function.

## Parameters / Member Variables
- : Link to the Aggref expression this state value serves (can be shared among multiple Aggrefs)
- : Indicates if this state value is shared by multiple Aggref expressions
- : True for ORDER BY and DISTINCT aggregates that are not pre-sorted
- : Total number of aggregated input columns including ORDER BY expressions
- : Number of input columns to pass to the transition function
- : OID of the state transition or combine function
- : OID of the serialization function (InvalidOid if none)
- : OID of the deserialization function (InvalidOid if none)
- : OID of the state value's data type
- : Function manager lookup data for the transition/combine function
- : Function manager lookup data for serialization function
- : Function manager lookup data for deserialization function
- : Input collation derived for the aggregate
- : Number of sorting columns
- : Number of columns for DISTINCT comparisons (0 or same as numSortCols)
- : Array of sort column indices (length numSortCols)
- : Array of sort operator OIDs (length numSortCols)
- : Array of sort collation OIDs (length numSortCols)
- : Array of null-first flags (length numSortCols)
- : Comparator function for single-column DISTINCT comparisons
- : Expression state for multi-column DISTINCT comparisons
- : Initial value from pg_aggregate catalog entry
- : Whether the initial value is NULL
- : Length of input data type
- : Length of transition data type
- : Whether input type is passed by value
- : Whether transition type is passed by value
- : Tuple slot for current input tuple (used for FILTER/ORDER BY/DISTINCT)
- : Tuple slot for multi-column DISTINCT processing
- : Tuple descriptor for input tuples
- : Last datum value for single-column DISTINCT checking
- : Whether last value was NULL for DISTINCT checking
- : Whether we have a last value for DISTINCT comparison
- : Array of Tuplesort objects for each grouping set
- : Pre-initialized FunctionCallInfo for transition function calls
- : Pre-initialized FunctionCallInfo for serialization function calls
- : Pre-initialized FunctionCallInfo for deserialization function calls

## Dependencies
- Functions called/Symbols referenced:
  - Aggref
  - Tuplesortstate
  - FunctionCallInfo
  - initValue
- Called from (representative examples):
  - ExecInitAgg
  - AggStatePerTrans

## Notes and Other Information
This structure is central to PostgreSQL's aggregate processing architecture, enabling efficient handling of various aggregate types. The separation between transition state (this struct) and final result computation (AggStatePerAggData) allows for optimization where multiple aggregates can share the same transition computation. The pre-initialized FunctionCallInfo structures help reduce per-row overhead during aggregate processing.