# AggStatePerTransData

## Location
[src/include/executor/nodeAgg.h:30-176](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/nodeAgg.h#L30-L176)

## Overview
AggStatePerTransData represents per-aggregate transition state value information, containing working state for updating an aggregate's state value by calling the transition function with input rows.

## Definition

```c
typedef struct AggStatePerTransData
{
	/*
	 * These values are set up during ExecInitAgg() and do not change
	 * thereafter:
	 */

	/*
	 * Link to an Aggref expr this state value is for.
	 *
	 * There can be multiple Aggref's sharing the same state value, so long as
	 * the inputs and transition functions are identical and the final
	 * functions are not read-write.  This points to the first one of them.
	 */
	Aggref	   *aggref;

	/*
	 * Is this state value actually being shared by more than one Aggref?
	 */
	bool		aggshared;

	/*
	 * True for ORDER BY and DISTINCT Aggrefs that are not aggpresorted.
	 */
	bool		aggsortrequired;

	/*
	 * Number of aggregated input columns.  This includes ORDER BY expressions
	 * in both the plain-agg and ordered-set cases.  Ordered-set direct args
	 * are not counted, though.
	 */
	int			numInputs;

	/*
	 * Number of aggregated input columns to pass to the transfn.  This
	 * includes the ORDER BY columns for ordered-set aggs, but not for plain
	 * aggs.  (This doesn't count the transition state value!)
	 */
	int			numTransInputs;

	/* Oid of the state transition or combine function */
	Oid			transfn_oid;

	/* Oid of the serialization function or InvalidOid */
	Oid			serialfn_oid;

	/* Oid of the deserialization function or InvalidOid */
	Oid			deserialfn_oid;

	/* Oid of state value's datatype */
	Oid			aggtranstype;

	/*
	 * fmgr lookup data for transition function or combine function.  Note in
	 * particular that the fn_strict flag is kept here.
	 */
	FmgrInfo	transfn;

	/* fmgr lookup data for serialization function */
	FmgrInfo	serialfn;

	/* fmgr lookup data for deserialization function */
	FmgrInfo	deserialfn;

	/* Input collation derived for aggregate */
	Oid			aggCollation;

	/* number of sorting columns */
	int			numSortCols;

	/* number of sorting columns to consider in DISTINCT comparisons */
	/* (this is either zero or the same as numSortCols) */
	int			numDistinctCols;

	/* deconstructed sorting information (arrays of length numSortCols) */
	AttrNumber *sortColIdx;
	Oid		   *sortOperators;
	Oid		   *sortCollations;
	bool	   *sortNullsFirst;

	/*
	 * Comparators for input columns --- only set/used when aggregate has
	 * DISTINCT flag. equalfnOne version is used for single-column
	 * comparisons, equalfnMulti for the case of multiple columns.
	 */
	FmgrInfo	equalfnOne;
	ExprState  *equalfnMulti;

	/*
	 * initial value from pg_aggregate entry
	 */
	Datum		initValue;
	bool		initValueIsNull;

	/*
	 * We need the len and byval info for the agg's input and transition data
	 * types in order to know how to copy/delete values.
	 *
	 * Note that the info for the input type is used only when handling
	 * DISTINCT aggs with just one argument, so there is only one input type.
	 */
	int16		inputtypeLen,
				transtypeLen;
	bool		inputtypeByVal,
				transtypeByVal;

	/*
	 * Slots for holding the evaluated input arguments.  These are set up
	 * during ExecInitAgg() and then used for each input row requiring either
	 * FILTER or ORDER BY/DISTINCT processing.
	 */
	TupleTableSlot *sortslot;	/* current input tuple */
	TupleTableSlot *uniqslot;	/* used for multi-column DISTINCT */
	TupleDesc	sortdesc;		/* descriptor of input tuples */
	Datum		lastdatum;		/* used for single-column DISTINCT */
	bool		lastisnull;		/* used for single-column DISTINCT */
	bool		haslast;		/* got a last value for DISTINCT check */

	/*
	 * These values are working state that is initialized at the start of an
	 * input tuple group and updated for each input tuple.
	 *
	 * For a simple (non DISTINCT/ORDER BY) aggregate, we just feed the input
	 * values straight to the transition function.  If it's DISTINCT or
	 * requires ORDER BY, we pass the input values into a Tuplesort object;
	 * then at completion of the input tuple group, we scan the sorted values,
	 * eliminate duplicates if needed, and run the transition function on the
	 * rest.
	 *
	 * We need a separate tuplesort for each grouping set.
	 */

	Tuplesortstate **sortstates;	/* sort objects, if DISTINCT or ORDER BY */

	/*
	 * This field is a pre-initialized FunctionCallInfo struct used for
	 * calling this aggregate's transfn.  We save a few cycles per row by not
	 * re-initializing the unchanging fields; which isn't much, but it seems
	 * worth the extra space consumption.
	 */
	FunctionCallInfo transfn_fcinfo;

	/* Likewise for serialization and deserialization functions */
	FunctionCallInfo serialfn_fcinfo;

	FunctionCallInfo deserialfn_fcinfo;
}			AggStatePerTransData;
```
## Detailed Description
AggStatePerTransData stores the working state needed to update an aggregate's transition state value. This struct is specifically designed for the transition phase of aggregate computation and does not contain information needed to produce the final result (which is stored in AggStatePerAggData). This separation enables multiple aggregate results to be produced from a single transition state.

The struct supports various aggregate types including simple aggregates, DISTINCT aggregates, and ORDER BY aggregates. For simple aggregates, input values are fed directly to the transition function. For DISTINCT or ORDER BY aggregates, input values are processed through a Tuplesort object, with duplicates eliminated and values sorted before applying the transition function.

## Parameters / Member Variables
- `aggref`: Link to the Aggref expression this state value serves (can be shared among multiple Aggrefs)
- `aggshared`: Indicates if this state value is shared by multiple Aggref expressions
- `aggsortrequired`: True for ORDER BY and DISTINCT aggregates that are not pre-sorted
- `numInputs`: Total number of aggregated input columns including ORDER BY expressions
- `numTransInputs`: Number of input columns to pass to the transition function
- `transfn_oid`: OID of the state transition or combine function
- `serialfn_oid`: OID of the serialization function (InvalidOid if none)
- `deserialfn_oid`: OID of the deserialization function (InvalidOid if none)
- `aggtranstype`: OID of the state value's data type
- `transfn`: Function manager lookup data for the transition/combine function
- `serialfn`: Function manager lookup data for serialization function
- `deserialfn`: Function manager lookup data for deserialization function
- `aggCollation`: Input collation derived for the aggregate
- `numSortCols`: Number of sorting columns
- `numDistinctCols`: Number of columns for DISTINCT comparisons (0 or same as numSortCols)
- `sortColIdx`: Array of sort column indices (length numSortCols)
- `sortOperators`: Array of sort operator OIDs (length numSortCols)
- `sortCollations`: Array of sort collation OIDs (length numSortCols)
- `sortNullsFirst`: Array of null-first flags (length numSortCols)
- `equalfnOne`: Comparator function for single-column DISTINCT comparisons
- `equalfnMulti`: Expression state for multi-column DISTINCT comparisons
- `initValue`: Initial value from pg_aggregate catalog entry
- `initValueIsNull`: Whether the initial value is NULL
- `inputtypeLen`: Length of input data type
- `transtypeLen`: Length of transition data type
- `inputtypeByVal`: Whether input type is passed by value
- `transtypeByVal`: Whether transition type is passed by value
- `sortslot`: Tuple slot for current input tuple (used for FILTER/ORDER BY/DISTINCT)
- `uniqslot`: Tuple slot for multi-column DISTINCT processing
- `sortdesc`: Tuple descriptor for input tuples
- `lastdatum`: Last datum value for single-column DISTINCT checking
- `lastisnull`: Whether last value was NULL for DISTINCT checking
- `haslast`: Whether we have a last value for DISTINCT comparison
- `sortstates`: Array of Tuplesort objects for each grouping set
- `transfn_fcinfo`: Pre-initialized FunctionCallInfo for transition function calls
- `serialfn_fcinfo`: Pre-initialized FunctionCallInfo for serialization function calls
- `deserialfn_fcinfo`: Pre-initialized FunctionCallInfo for deserialization function calls

## Dependencies
- Functions called/Symbols referenced:
  - [Aggref](Aggref.md)
  - [Tuplesortstate](../T/Tuplesortstate.md)
  - [FunctionCallInfo](../F/FunctionCallInfo.md)
  - [initValue](../i/initValue.md)
- Called from (representative examples):
  - [ExecInitAgg](../E/ExecInitAgg.md)
  - [AggStatePerTrans](AggStatePerTrans.md)

## Notes and Other Information
This structure is central to PostgreSQL's aggregate processing architecture, enabling efficient handling of various aggregate types. The separation between transition state (this struct) and final result computation (AggStatePerAggData) allows for optimization where multiple aggregates can share the same transition computation. The pre-initialized FunctionCallInfo structures help reduce per-row overhead during aggregate processing.