# query_planner

## Location
src/backend/optimizer/plan/planmain.c: 54 - 288

## Overview
The main entry point for generating a simplified execution path for a basic query that may involve joins but not advanced features like grouping or sorting.

## Definition


## Detailed Description
The  function is a core component of PostgreSQL's query optimizer responsible for generating access paths for basic query execution. Located at , it serves as the foundation for query planning by handling the core join planning phase.

The function operates in several key phases:

1. **Initialization**: Sets up planner data structures and arrays for accessing base relations
2. **Trivial Query Optimization**: Handles simple cases like "SELECT expression" with optimized Result paths
3. **Base Relation Construction**: Builds RelOptInfo nodes for all base relations in the query
4. **Clause Processing**: Analyzes targetlists, join trees, and builds equivalence classes for optimization
5. **Join Planning**: Calls  to generate the optimal join plan
6. **Validation**: Ensures at least one usable execution path was found

The function does not handle top-level processing features like grouping, sorting, or aggregation - these are handled by the caller (). Instead, it returns a  representing the top level of joining, allowing the caller to choose among surviving paths.

## Parameters / Member Variables
- : PlannerInfo structure describing the query to plan, containing parse tree and planning context
- : Callback function to compute query_pathkeys once equivalence class merging is complete
- : Optional extra data to pass to the callback function

## Dependencies
- Functions called/Symbols referenced:
  - setup_simple_rel_arrays
  - build_simple_rel
  - is_parallel_safe
  - add_path
  - create_group_result_path
  - set_cheapest
  - add_base_rels_to_query
  - build_base_rel_tlists
  - find_placeholders_in_jointree
  - find_lateral_references
  - deconstruct_jointree
  - reconsider_outer_join_clauses
  - generate_base_implied_equalities
  - fix_placeholder_input_needed_levels
  - remove_useless_joins
  - reduce_unique_semijoins
  - add_placeholders_to_base_rels
  - create_lateral_join_info
  - match_foreign_keys_to_quals
  - extract_restriction_or_clauses
  - add_other_rels_to_query
  - distribute_row_identity_vars
  - make_one_rel
- Called from (representative examples):
  - grouping_planner
  - build_minmax_path

## Notes and Other Information
- The function includes a special optimization path for trivial queries (single RTE_RESULT relations) that bypasses most of the complex planning logic
- Equivalence class merging must be completed before canonical pathkeys can be generated, which is why the callback mechanism is used
- The function performs extensive join optimization including removal of useless outer joins and reduction of unique semijoins
- Placeholder expressions from subquery pullup are carefully managed to ensure proper variable marking at appropriate join levels
- The final validation ensures that at least one usable path exists and that it doesn't require parameters (indicating a planning failure)
- Foreign key relationships are matched to equivalence classes and join quals for additional optimization opportunities
- Appendrel (inheritance/partitioning) expansion is delayed until the end to maximize available information for pruning