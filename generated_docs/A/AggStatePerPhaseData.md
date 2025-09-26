# AggStatePerPhaseData

## Location
src/include/executor/nodeAgg.h: 280 - 300

## Overview
AggStatePerPhaseData represents per-grouping-set-phase state for processing grouping sets in multiple passes, with each phase containing grouping sets that can be processed together in a single data pass.

## Definition

```c
typedef struct AggStatePerPhaseData
{
	AggStrategy aggstrategy;	/* strategy for this phase */
	int			numsets;		/* number of grouping sets (or 0) */
	int		   *gset_lengths;	/* lengths of grouping sets */
	Bitmapset **grouped_cols;	/* column groupings for rollup */
	ExprState **eqfunctions;	/* expression returning equality, indexed by
								 * nr of cols to compare */
	Agg		   *aggnode;		/* Agg node for phase data */
	Sort	   *sortnode;		/* Sort node for input ordering for phase */

	ExprState  *evaltrans;		/* evaluation of transition functions  */

	/*----------
	 * Cached variants of the compiled expression.
	 * first subscript: 0: outerops; 1: TTSOpsMinimalTuple
	 * second subscript: 0: no NULL check; 1: with NULL check
	 *----------
	 */
	ExprState  *evaltrans_cache[2][2];
}			AggStatePerPhaseData;
```
## Detailed Description
AggStatePerPhaseData manages the state for processing grouping sets in phases, where each phase represents a collection of grouping sets that can be computed in a single pass over the input data. When multiple phases are required, the system resets state after completing each phase and performs another pass over re-sorted data.

The structure supports different aggregation strategies per phase and maintains cached expression variants for performance optimization. The evaltrans_cache provides precompiled expression variants based on tuple slot operations and NULL checking requirements, reducing runtime compilation overhead.

Each phase after the first requires a sort order, which is specified by the sortnode field. This multi-phase approach enables efficient processing of complex grouping set queries that would otherwise require multiple separate query executions.

## Parameters / Member Variables
- : Aggregation strategy used for this specific phase (e.g., AGG_PLAIN, AGG_SORTED, AGG_HASHED)
- : Number of grouping sets in this phase (0 for simple aggregates)
- : Array containing the length of each grouping set
- : Array of Bitmapsets representing column groupings for rollup operations
- : Array of ExprState pointers for equality functions, indexed by number of columns to compare
- : Pointer to the Agg node containing phase-specific aggregate information
- : Pointer to the Sort node defining input ordering requirements for this phase
- : ExprState for evaluating transition functions during aggregate computation
- : 2x2 cache array of compiled expression variants for performance optimization
  - First dimension: 0=outerops, 1=TTSOpsMinimalTuple (tuple slot operations)
  - Second dimension: 0=no NULL check, 1=with NULL check

## Dependencies
- Functions called/Symbols referenced:
  - AggStrategy
  - Agg
  - Sort
- Called from (representative examples):
  - ExecInitAgg
  - ExecProcNode
  - AggStatePerPhase

## Notes and Other Information
The phase-based approach to grouping sets is a key optimization in PostgreSQL's aggregate processing. By grouping compatible grouping sets into phases, the system minimizes the number of data passes required while maintaining correct results. The cached expression variants (evaltrans_cache) provide significant performance benefits by avoiding repeated expression compilation for common tuple slot operation and NULL checking patterns during aggregate evaluation.