# approx_tuple_count

## Location
src/backend/optimizer/path/costsize.c: 5197 - 5241

## Overview
Provides a quick-and-dirty estimation of the number of join rows passing a set of qualification conditions by multiplying independent clause selectivities.

## Definition


## Detailed Description
This function estimates how many tuples will pass through initial merge or hash join steps by applying qualification conditions. It uses a simplified approach that:

1. **Bypasses clauselist_selectivity**: Instead of using the more sophisticated clauselist_selectivity function, it simply multiplies individual clause selectivities together
2. **Uses JOIN_INNER semantics**: Intentionally computes selectivity under JOIN_INNER rules regardless of the actual join type, since it's estimating tuples passing the initial join step
3. **Leverages caching**: Individual clause selectivities can be cached effectively, unlike clauselist_selectivity results
4. **Applies approximation**: Accepts less precision for better performance since results are only used to estimate potential output tuples

The function creates a dummy SpecialJoinInfo structure for JOIN_INNER semantics, iterates through each qualification clause to compute selectivity, multiplies all selectivities together, applies this to the Cartesian product of input relation sizes, and clamps the result to a reasonable range.

## Parameters / Member Variables
- : PlannerInfo structure containing global planner state
- : JoinPath representing the join operation being estimated
- : List of qualification conditions (boolean expressions or RestrictInfo nodes)

## Dependencies
- Functions called/Symbols referenced:
  - [init_dummy_sjinfo](../i/init_dummy_sjinfo.md)
  - [clause_selectivity](../c/clause_selectivity.md)
  - [clamp_row_est](../c/clamp_row_est.md)
  - JoinPath
  - [SpecialJoinInfo](../S/SpecialJoinInfo.md)
  - JOIN_INNER
- Called from (representative examples):
  - [final_cost_mergejoin](../f/final_cost_mergejoin.md)
  - [final_cost_hashjoin](../f/final_cost_hashjoin.md)
  - cost_qual_eval_context

## Notes and Other Information
- This is a static function used internally within costsize.c for performance optimization
- Trades accuracy for speed by using independent clause multiplication rather than sophisticated dependency analysis
- Results are only used for estimating potential output tuple counts, not final selectivity calculations
- The simplification is acceptable because many situations can't do better than independent multiplication anyway
- Caching benefits make this approach more efficient for repeated cost calculations
- Located in src/backend/optimizer/path/costsize.c:5197-5241