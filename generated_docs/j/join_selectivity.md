# join_selectivity

## Location
[src/backend/optimizer/util/plancat.c:1986-2026](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/plancat.c#L1986-L2026)

## Overview
Calculates the selectivity of a join operator clause by invoking the operator's registered join selectivity estimation procedure.

## Definition


## Detailed Description
This function computes the selectivity estimate for a join condition by calling the join selectivity estimation function associated with the specified operator. Join selectivity represents the fraction of the Cartesian product between two relations that is expected to satisfy the join condition.

The function retrieves the operator's join selectivity procedure () from the system catalog and invokes it through the function manager. If no join selectivity function is registered for the operator, it defaults to a conservative estimate of 0.5 (50% selectivity).

The join selectivity function receives comprehensive context information including the planner state, operator details, join arguments, join type (inner, left, right, full outer), and special join information for outer joins. This allows for sophisticated selectivity estimates that consider join semantics and statistical correlations between joined tables.

## Parameters / Member Variables
- : PlannerInfo containing global planner state, statistics, and relation information
- : Object ID of the join operator for which to estimate selectivity
- : List of arguments (operands) to the join operator clause
- : Collation ID for string comparison operations in the join condition
- : Type of join operation (JoinType enum: INNER, LEFT, RIGHT, FULL, etc.)
- : SpecialJoinInfo structure containing additional context for outer joins

## Dependencies
- Functions called/Symbols referenced:
  - [get_oprjoin](../g/get_oprjoin.md) (retrieves operator's join selectivity function)
  - [OidFunctionCall5Coll](../O/OidFunctionCall5Coll.md) (invokes the selectivity function with 5 parameters and collation)
  - [DatumGetFloat8](../D/DatumGetFloat8.md) (converts function result to float8)
  - RegProcedure (procedure identifier type)
  - JoinType, SpecialJoinInfo (join-related data structures)
  - [PointerGetDatum](../P/PointerGetDatum.md), ObjectIdGetDatum, Int16GetDatum (datum conversion functions)

- Called from (representative examples):
  - [clause_selectivity_ext](../c/clause_selectivity_ext.md) (src/backend/optimizer/path/clausesel.c:839)
  - [rowcomparesel](../r/rowcomparesel.md) (src/backend/utils/adt/selfuncs.c:2251)
  - [test_support_func](../t/test_support_func.md) (src/test/regress/regress.c:1045)

## Notes and Other Information
- Returns default selectivity of 0.5 when no  procedure is registered
- Validates that returned selectivity is within valid range [0.0, 1.0]
- Critical for join order optimization and join cost estimation
- Different operators have specialized join selectivity functions (e.g., eqjoinsel for equality joins)
- Considers join type semantics (outer joins may have different selectivity characteristics)
- [SpecialJoinInfo](../S/SpecialJoinInfo.md) parameter provides context for complex join scenarios
- Essential component of PostgreSQL's join planning and optimization system
- Location: src/backend/optimizer/util/plancat.c:1986-2026