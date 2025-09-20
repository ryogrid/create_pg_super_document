# consider_index_join_clauses

## Location
[src/backend/optimizer/path/indxpath.c:431-496](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/indxpath.c#L431-L496)

## Overview
Decides which parameterized index paths to build given sets of join clauses for an index, by identifying useful combinations of outer relations that can provide indexable join clauses.

## Definition

```c
static void
consider_index_join_clauses(PlannerInfo *root, RelOptInfo *rel,
							IndexOptInfo *index,
							IndexClauseSet *rclauseset,
							IndexClauseSet *jclauseset,
							IndexClauseSet *eclauseset,
							List **bitindexpaths)
```
## Detailed Description
This function implements a strategy for generating parameterized index paths by systematically considering different combinations of outer relations that can provide indexable join clauses. The core approach is to:

1. Identify every potentially useful set of outer relations that can provide indexable join clauses
2. For each such set, select all available join clauses from those outer relations
3. Add all indexable restriction clauses to the mix
4. Generate plain and/or bitmap index paths for each combination

The function operates under the assumption that it's always better to apply a clause as an indexqual rather than as a filter (qpqual). It includes a heuristic safety valve to limit the number of outer relation sets considered to prevent exponential explosion in complex queries.

The function processes both simple join clauses (jclauseset) and EquivalenceClass-derived join clauses (eclauseset) for each index column, delegating the actual path generation to consider_index_join_outer_rels.

## Parameters / Member Variables
- : PlannerInfo containing query planning context
- : RelOptInfo for the index's heap relation  
- : IndexOptInfo for the index to generate paths for
- : IndexClauseSet containing indexable restriction clauses
- : IndexClauseSet containing indexable simple join clauses
- : IndexClauseSet containing indexable clauses from EquivalenceClasses
- : Output list to add bitmap index paths to for later processing

## Dependencies
- Functions called/Symbols referenced:
  - [consider_index_join_outer_rels](consider_index_join_outer_rels.md)
- Called from (representative examples):
  - [create_index_paths](create_index_paths.md)

## Notes and Other Information
- Uses a heuristic to limit computational complexity: restricts the number of outer relation sets considered to a multiple of the number of clauses
- Always considers using each individual join clause, even when applying the safety heuristic
- Represents each set of outer relations as a maximum set of clause_relids (including the indexed relation itself)
- Maintains considered_relids list to avoid redundant processing of the same relation combinations
- Plain index paths are sent directly to add_path(), while bitmap paths are collected for later batch processing