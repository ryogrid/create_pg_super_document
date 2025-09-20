# eqjoinsel_inner

## Location
[src/backend/utils/adt/selfuncs.c:2438-2634](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L2438-L2634)

## Overview
Computes join selectivity for normal inner joins (and LEFT/FULL outer joins) using detailed statistical analysis, particularly leveraging Most Common Values (MCVs) when available for highly accurate estimates.

## Definition

```c
struct just once. Using
		 * FunctionCallInvoke directly also avoids failure if the eqproc
		 * returns NULL, though really equality functions should never do
		 * that.
		 */
		InitFunctionCallInfoData(*fcinfo, &eqproc, 2, collation,
								 NULL, NULL);
```
## Detailed Description
This function implements the core logic for estimating equality join selectivity using two distinct approaches:

**MCV-based estimation (when both sides have MCVs):**
- Performs actual equality comparisons between MCV lists from both relations
- Calculates exact selectivity for the portion represented by MCVs
- Estimates selectivity for non-MCV populations using statistical extrapolation
- Uses the mathematical framework from Ioannidis and Christodoulakis research on join size estimation accuracy

**Statistical estimation (fallback approach):**
- Uses the formula MIN(1/nd1, 1/nd2) * (1-nullfrac1) * (1-nullfrac2)
- Assumes equal distribution of non-null values
- Takes the minimum to estimate from the perspective of the relation with smaller distinct value count

The MCV approach provides significantly higher accuracy for skewed distributions by computing exact matches for the most frequent values, then extrapolating to estimate the remaining population.

## Parameters / Member Variables
- : OID of the equality comparison function
- : Collation to use for string comparisons
- , : Variable statistical data for both join sides
- , : Number of distinct values for each variable
- , : Whether the distinct value counts are defaults
- , : Attribute statistics slots containing MCV data
- , : PostgreSQL statistics forms with null fractions
- , : Whether MCV statistics are available for each side

## Dependencies
- Functions called/Symbols referenced:
  - [fmgr_info](../f/fmgr_info.md)
  - InitFunctionCallInfoData
  - FunctionCallInvoke
  - CLAMP_PROBABILITY
  - LOCAL_FCINFO
- Called from (representative examples):
  - [eqjoinsel](eqjoinsel.md)

## Notes and Other Information
- The function assumes each MCV matches at most one member of the other MCV list for performance and mathematical consistency
- Memory is carefully managed with palloc0 for temporary arrays and pfree for cleanup
- Probability values are clamped to ensure they remain within valid [0,1] bounds
- The algorithm handles null fractions explicitly in both estimation approaches
- For relations without MCV statistics, the function falls back to a conservative but mathematically sound estimate
- The choice of smaller estimate between totalsel1 and totalsel2 provides a more reliable bound by considering the limiting relation
- This function is also used for LEFT and FULL outer joins since the additional complexity of distinguishing them isn't currently deemed worthwhile