# OuterJoinClauseInfo

## Location
[src/include/nodes/pathnodes.h:2920-2927](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L2920-L2927)

## Overview
OuterJoinClauseInfo is a transient structure used during query planning to track mergejoinable outer join ON clauses that require special processing at the end of qual distribution.

## Definition


## Detailed Description
OuterJoinClauseInfo structures are created temporarily during the qualification distribution phase of query planning. The planner sets aside every outer join ON clause that appears to be mergejoinable, storing it in this structure for specialized processing after the main qual distribution is complete.

This delayed processing is necessary because outer join ON clauses have special semantics that can affect join ordering and optimization decisions. By collecting them separately, the planner can apply more sophisticated analysis and potentially derive additional equivalence classes or optimization opportunities.

The structure serves as a bridge between the restrictinfo representation of the clause and the broader join information needed for optimization decisions.

## Parameters / Member Variables
- : Node tag for structure identification
- : Pointer to RestrictInfo containing the mergejoinable outer join ON clause
- : Pointer to the associated SpecialJoinInfo structure for the outer join

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (node identification)
  - [RestrictInfo](../R/RestrictInfo.md) (clause restriction information)
  - [SpecialJoinInfo](../S/SpecialJoinInfo.md) (outer join metadata)

- Called from (representative examples):
  - [distribute_qual_to_rels](../d/distribute_qual_to_rels.md)
  - [reconsider_outer_join_clauses](../r/reconsider_outer_join_clauses.md)
  - [reconsider_outer_join_clause](../r/reconsider_outer_join_clause.md)
  - [reconsider_full_join_clause](../r/reconsider_full_join_clause.md)

## Notes and Other Information
- Transient structure used only during planning phase, not persisted in final plans
- Specifically targets mergejoinable clauses which have potential for optimization
- Part of the qual distribution process that handles complex outer join semantics
- Enables deferred processing of outer join ON clauses after initial qual distribution
- Critical for proper handling of equivalence class generation in the presence of outer joins
- The no_copy_equal attribute indicates this structure doesn't participate in standard node copying/comparison operations