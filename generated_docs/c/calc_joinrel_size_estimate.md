# calc_joinrel_size_estimate

## Location
[src/backend/optimizer/path/costsize.c:5394-5543](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L5394-L5543)

## Overview
The core workhorse function that calculates join size estimates by applying selectivity computations to the Cartesian product of input relations, with special handling for different join types.

## Definition
```c
static double calc_joinrel_size_estimate(PlannerInfo *root, RelOptInfo *joinrel,
                                         RelOptInfo *outer_rel, RelOptInfo *inner_rel,
                                         double outer_rows, double inner_rows,
                                         SpecialJoinInfo *sjinfo, List *restrictlist)
```

## Detailed Description
This static function performs the actual computation of join cardinality estimates used by both `set_joinrel_size_estimates` and `get_parameterized_joinrel_size`. It implements a sophisticated algorithm that considers foreign key relationships, different join types, and the distinction between join conditions and pushed-down predicates.

The estimation process follows these key steps:

1. **Foreign Key Analysis**: Uses `get_foreign_key_join_selectivity` to identify and specially handle join clauses that match known foreign key constraints, providing more accurate estimates than generic selectivity calculations.

2. **Clause Classification**: For outer joins, separates restriction clauses into join conditions (ON/WHERE clauses) and pushed-down conditions, as they have different selectivity impacts.

3. **Selectivity Calculation**: Applies `clauselist_selectivity` to compute selectivity values for the relevant clause groups.

4. **Join-Type-Specific Logic**: Implements different cardinality formulas for each join type:
   - **INNER**: Standard Cartesian product × selectivity
   - **LEFT/FULL**: Ensures result is at least as large as the non-nullable side(s)
   - **SEMI**: Returns fraction of outer rows with matches
   - **ANTI**: Returns fraction of outer rows without matches

5. **Result Clamping**: Uses `clamp_row_est` to ensure the result is within reasonable bounds.

## Parameters / Member Variables
- `root`: PlannerInfo containing global planning context
- `joinrel`: The join RelOptInfo being estimated  
- `outer_rel`: Outer input relation metadata
- `inner_rel`: Inner input relation metadata
- `outer_rows`: Actual number of rows from outer relation (may differ from outer_rel->rows for parameterized paths)
- `inner_rows`: Actual number of rows from inner relation (may differ from inner_rel->rows for parameterized paths)
- `sjinfo`: SpecialJoinInfo describing the join type and constraints
- `restrictlist`: List of restriction clauses to be applied at this join

## Dependencies
- Functions called/Symbols referenced:
  - [get_foreign_key_join_selectivity](../g/get_foreign_key_join_selectivity.md)
  - IS_OUTER_JOIN
  - RINFO_IS_PUSHED_DOWN  
  - [clauselist_selectivity](clauselist_selectivity.md)
  - [list_free](../l/list_free.md)
  - [clamp_row_est](clamp_row_est.md)
  - JoinType, SpecialJoinInfo (types)
  - JOIN_INNER, JOIN_LEFT, JOIN_FULL, JOIN_SEMI, JOIN_ANTI (enum values)
- Called from (representative examples):
  - [set_joinrel_size_estimates](../s/set_joinrel_size_estimates.md) (src/backend/optimizer/path/costsize.c:5327)
  - [get_parameterized_joinrel_size](../g/get_parameterized_joinrel_size.md) (src/backend/optimizer/path/costsize.c:5370)

## Notes and Other Information
- This is a static function, only accessible within the costsize.c module
- Handles the complexity of distinguishing join conditions from pushed-down predicates in outer joins
- Foreign key relationships receive special treatment for improved estimation accuracy
- [Result](../R/Result.md) clamping prevents unrealistic estimates that could mislead the optimizer
- The function carefully manages memory by freeing temporary lists to prevent leaks
- Different join types have fundamentally different cardinality semantics that are properly reflected in the calculations