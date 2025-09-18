# set_rel_size

## Location
[src/backend/optimizer/path/allpaths.c:360-468](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L360-L468)

## Overview
Sets size estimates for a base relation by analyzing the relation type and characteristics, handling various cases including constraint exclusion, inheritance, and different RTE types.

## Definition


## Detailed Description
This function serves as the central dispatcher for establishing size estimates across different types of relations in PostgreSQL. It implements a comprehensive logic tree that handles:

1. **Constraint Exclusion**: Checks if the relation can be excluded entirely via constraints, setting up dummy paths when possible
2. **Inheritance Relations**: Processes append relations (tables with inheritance) using specialized logic
3. **Relation Type Dispatch**: Routes different RTE types to their appropriate sizing functions:
   - Regular tables, foreign tables, partitioned tables, sampled relations
   - Subqueries, functions, table functions, VALUES clauses
   - CTEs (including self-referencing worktables), named tuplestores, result relations

For some relation types (subqueries, CTEs, named tuplestores, result relations), the function immediately builds paths instead of just setting sizes, as these types don't support parameterized vs unparameterized path choices.

## Parameters / Member Variables
- : PlannerInfo structure containing global optimizer state and query information
- : RelOptInfo structure for the relation being sized
- : Range table index identifying the relation
- : RangeTblEntry containing relation metadata and properties

## Dependencies
- Functions called/Symbols referenced:
  - [relation_excluded_by_constraints](../r/relation_excluded_by_constraints.md)
  - [set_dummy_rel_pathlist](set_dummy_rel_pathlist.md)
  - [set_append_rel_size](set_append_rel_size.md)
  - [set_foreign_size](set_foreign_size.md)
  - [set_tablesample_rel_size](set_tablesample_rel_size.md)
  - [set_plain_rel_size](set_plain_rel_size.md)
  - [set_subquery_pathlist](set_subquery_pathlist.md)
  - [set_function_size_estimates](set_function_size_estimates.md)
  - [set_tablefunc_size_estimates](set_tablefunc_size_estimates.md)
  - [set_values_size_estimates](set_values_size_estimates.md)
  - [set_worktable_pathlist](set_worktable_pathlist.md)
  - [set_cte_pathlist](set_cte_pathlist.md)
  - [set_namedtuplestore_pathlist](set_namedtuplestore_pathlist.md)
  - [set_result_pathlist](set_result_pathlist.md)
  - Various RTE type constants (RTE_RELATION, RTE_SUBQUERY, etc.)
- Called from (representative examples):
  - [set_base_rel_sizes](set_base_rel_sizes.md)
  - [set_append_rel_size](set_append_rel_size.md)

## Notes and Other Information
- Located in src/backend/optimizer/path/allpaths.c:360-468
- Static function, only used within the allpaths.c module
- Includes an assertion ensuring all non-dummy relations have nonzero rowcount estimates
- Handles special case of partitioned tables with ONLY clause by marking them as dummy relations
- Critical function in the size estimation pipeline, serving as the primary dispatcher for different relation types
- Some relation types bypass the usual size-then-paths sequence by building paths immediately during sizing