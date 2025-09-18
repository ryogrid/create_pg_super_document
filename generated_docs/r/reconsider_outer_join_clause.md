# reconsider_outer_join_clause

## Location
[src/backend/optimizer/path/equivclass.c:2114-2236](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/equivclass.c#L2114-L2236)

## Overview
Processes a single LEFT/RIGHT JOIN clause to determine if constant values can be safely propagated from the outer relation to the inner relation through equivalence classes.

## Definition
```c
static bool reconsider_outer_join_clause(PlannerInfo *root, OuterJoinClauseInfo *ojcinfo, bool outer_on_left)
```

## Detailed Description
This function implements the core logic for optimizing LEFT and RIGHT JOIN clauses by leveraging transitivity in equivalence relationships. Given an outer join clause OUTERVAR = INNERVAR, it searches for existing equivalence classes where OUTERVAR = CONSTANT, enabling the safe derivation of INNERVAR = CONSTANT constraints that can be pushed into the inner relation.

The function extracts the outer and inner variables based on the join direction, then searches through all equivalence classes for matches. For each matching equivalence class containing constants, it generates new equality clauses between the inner variable and each constant, using select_equality_operator to find appropriate operators and build_implied_join_equality to construct the RestrictInfo structures.

The optimization is safe because any inner rows not meeting the constant constraint cannot contribute to the join result anyway, as they would be filtered out by the corresponding outer relation constraint.

## Parameters / Member Variables
- `root`: Pointer to the PlannerInfo containing global planning state
- `ojcinfo`: OuterJoinClauseInfo structure containing the outer join clause details and associated special join information
- `outer_on_left`: Boolean indicating whether the outer relation is on the left side of the join clause

## Dependencies
- Functions called/Symbols referenced:
  - [is_opclause](../i/is_opclause.md) (checks if clause is an operator expression)
  - [op_input_types](../o/op_input_types.md) (extracts operator input data types)
  - [get_leftop](../g/get_leftop.md), get_rightop (extract operands from expressions)
  - [select_equality_operator](../s/select_equality_operator.md) (finds suitable equality operators)
  - [build_implied_join_equality](../b/build_implied_join_equality.md) (constructs new RestrictInfo clauses)
  - [find_join_domain](../f/find_join_domain.md) (locates appropriate join domain)
  - [process_equivalence](../p/process_equivalence.md) (processes the new equivalence relationship)
  - [equal](../e/equal.md) (tests expression equality)
  - [bms_copy](../b/bms_copy.md) (copies relation bitmaps)
- Called from (representative examples):
  - [reconsider_outer_join_clauses](reconsider_outer_join_clauses.md) (main outer join processing loop)

## Notes and Other Information
- Returns true if constant propagation was successful, false otherwise
- Only processes equivalence classes that contain constants (ec_has_const = true)
- Avoids volatile equivalence classes to maintain correctness
- Validates semantic compatibility through collation and operator family matching
- Generates constraints within the appropriate JoinDomain for the outer join
- Each successful constant propagation enables the parent function to remove the original outer join clause
- The function ensures that at least one constant constraint is successfully generated before declaring success