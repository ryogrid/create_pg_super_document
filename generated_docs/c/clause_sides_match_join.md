# clause_sides_match_join

## Location
[src/backend/optimizer/plan/analyzejoins.c:128-159](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/analyzejoins.c#L128-L159)

## Overview
Determines whether a join clause has the correct form to be used in a specific join by checking if the clause's operands properly separate outer and inner relation variables.

## Definition


## Detailed Description
This function validates that a binary operation clause is suitable for a particular join configuration. It ensures that the clause has a proper "outer_expr op inner_expr" or "inner_expr op outer_expr" structure, where each side of the operator references variables from only one side of the join (either outer or inner relations), not a mixture of both.

The function performs the validation by:
1. Checking if the left operand references only outer relation variables and the right operand references only inner relation variables
2. Checking the reverse case where left operand references only inner relation variables and right operand references only outer relation variables
3. Setting the transient flag  to indicate which side of the clause corresponds to the outer relation

This validation is crucial for ensuring that join clauses can be properly used by various join algorithms (hash joins, merge joins, etc.).

## Parameters / Member Variables
- : RestrictInfo structure containing information about the join clause, including which relations are referenced by each operand
- : RelOptInfo structure representing the outer relation in the join
- : RelOptInfo structure representing the inner relation in the join

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_subset](../b/bms_is_subset.md): Checks if one bitmap set is a subset of another (used to verify relid containment)

- Called from (representative examples):
  - [hash_inner_and_outer](../h/hash_inner_and_outer.md): Hash join path creation
  - [select_mergejoin_clauses](../s/select_mergejoin_clauses.md): Merge join clause selection
  - [join_is_removable](../j/join_is_removable.md): Join elimination analysis
  - [is_innerrel_unique_for](../i/is_innerrel_unique_for.md): Uniqueness analysis for join removal

## Notes and Other Information
- This is a static inline function, optimized for performance due to frequent usage in join planning
- The function modifies the  field in the RestrictInfo structure as a side effect
- Essential for join algorithm selection and optimization, as different join types require specific clause orientations
- The function assumes the clause is already known to be a binary operation referencing only the relations involved in the current join
- Returns false if the clause mixes outer and inner relation variables on either side, making it unsuitable for the join