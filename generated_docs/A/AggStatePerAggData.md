# AggStatePerAggData

## Location
[src/include/executor/nodeAgg.h:187-232](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/nodeAgg.h#L187-L232)

## Overview
AggStatePerAggData contains per-aggregate information needed to produce final aggregate results from transition state values, with support for sharing among multiple identical Aggrefs.

## Definition

```c
typedef struct AggStatePerAggData
{
	/*
	 * Link to an Aggref expr this state value is for.
	 *
	 * There can be multiple identical Aggref's sharing the same per-agg. This
	 * points to the first one of them.
	 */
	Aggref	   *aggref;

	/* index to the state value which this agg should use */
	int			transno;

	/* Optional Oid of final function (may be InvalidOid) */
	Oid			finalfn_oid;

	/*
	 * fmgr lookup data for final function --- only valid when finalfn_oid is
	 * not InvalidOid.
	 */
	FmgrInfo	finalfn;

	/*
	 * Number of arguments to pass to the finalfn.  This is always at least 1
	 * (the transition state value) plus any ordered-set direct args. If the
	 * finalfn wants extra args then we pass nulls corresponding to the
	 * aggregated input columns.
	 */
	int			numFinalArgs;

	/* ExprStates for any direct-argument expressions */
	List	   *aggdirectargs;

	/*
	 * We need the len and byval info for the agg's result data type in order
	 * to know how to copy/delete values.
	 */
	int16		resulttypeLen;
	bool		resulttypeByVal;

	/*
	 * "shareable" is false if this agg cannot share state values with other
	 * aggregates because the final function is read-write.
	 */
	bool		shareable;
}			AggStatePerAggData;
```
## Detailed Description
AggStatePerAggData stores the information required to call the final function and produce a final aggregate result from the transition state value. This struct is complementary to AggStatePerTransData - while the latter handles the transition phase, this struct manages the finalization phase of aggregate computation.

Multiple identical Aggref expressions in a query can share the same AggStatePerAggData instance, enabling optimization by avoiding duplicate final function calls. The struct contains metadata about the final function, result type information, and direct arguments for ordered-set aggregates.

All values in this structure are set up during ExecInitAgg() and remain unchanged throughout query execution, making it a read-only configuration structure for the finalization phase.

## Parameters / Member Variables
- `*aggref`: Link to the Aggref expression this aggregate data serves (points to first of potentially multiple identical Aggrefs)
- `transno`: Index to the transition state value that this aggregate should use
- `finalfn_oid`: OID of the final function (may be InvalidOid if no final function needed)
- `finalfn`: Function manager lookup data for the final function (valid only when finalfn_oid is not InvalidOid)
- `numFinalArgs`: Number of arguments to pass to the final function (minimum 1 for transition state plus any ordered-set direct args)
- `*aggdirectargs`: List of ExprStates for direct-argument expressions used in ordered-set aggregates
- `resulttypeLen`: Length of the aggregate's result data type for memory management
- `resulttypeByVal`: Whether the result type is passed by value for efficient copying
- `shareable`: False if this aggregate cannot share state values due to read-write final function
## Dependencies
- Functions called/Symbols referenced:
  - [Aggref](Aggref.md)
- Called from (representative examples):
  - [ExecInitAgg](../E/ExecInitAgg.md)
  - [AggStatePerAgg](AggStatePerAgg.md)

## Notes and Other Information
This structure is essential for PostgreSQL's two-phase aggregate processing architecture. The separation between transition computation (AggStatePerTransData) and result finalization (AggStatePerAggData) enables important optimizations like state sharing among identical aggregates. The shareable flag prevents incorrect results when final functions modify their input, ensuring data integrity during parallel aggregate processing.