# networkjoinsel

## Location
src/backend/utils/adt/network_selfuncs.c: 196 - 262

## Overview
Estimates join selectivity for network subnet inclusion/overlap operators, used by PostgreSQL query planner to predict result size when joining tables on network conditions.

## Definition
```c
Datum networkjoinsel(PG_FUNCTION_ARGS)
```

## Detailed Description
The `networkjoinsel` function provides join selectivity estimation for network data type operators (inet, cidr) when used in join conditions. It follows the same structural pattern as `eqjoinsel` but is specialized for network operations like subnet containment and overlap.

The function dispatches to specialized handlers based on join type: `networkjoinsel_inner` for inner/left/full joins, and `networkjoinsel_semi` for semi/anti joins. It includes performance optimizations to handle large statistics by limiting consideration to at most 1024 elements (MAX_CONSIDERED_ELEMS) from MCV and histogram arrays.

For histogram processing, the function uses decimation (sampling every k-th element) to maintain reasonable performance while preserving statistical accuracy. This approach ensures O(N^2) complexity doesn't become prohibitive with large statistics targets.

## Parameters / Member Variables
- `root`: PlannerInfo pointer containing query planning context
- `operator`: OID of the network operator being evaluated
- `args`: List of join arguments
- `jointype`: Type of join operation (unused in current implementation)
- `sjinfo`: SpecialJoinInfo containing join-specific information

## Dependencies
- Functions called/Symbols referenced:
  - get_join_variables
  - networkjoinsel_inner
  - networkjoinsel_semi
  - get_commutator
  - ReleaseVariableStats
  - CLAMP_PROBABILITY
- Called from (representative examples):
  - Used as join selectivity function registered in system catalogs
  - Invoked by query planner during join optimization

## Notes and Other Information
- Treats left/full join selectivity the same as inner join (following eqjoinsel pattern)
- For semi/anti joins, ensures outer variable is passed on the left side
- Uses operator commutation when join variables are reversed
- Implements performance limits to prevent excessive computation with large statistics
- Results are clamped to valid probability range [0.0, 1.0]
- Contains detailed comments about O(N^2) performance considerations and mitigation strategies