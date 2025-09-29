# query_planner

## Location
[src/backend/optimizer/plan/planmain.c:54-288](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planmain.c#L54-L288)

## Overview
The main entry point for generating a simplified execution path for a basic query that may involve joins but not advanced features like grouping or sorting.

## Definition

```c
struct RelOptInfo nodes for all base relations used in the query.
	 * Appendrel member relations ("other rels") will be added later.
	 *
	 * Note: the reason we find the baserels by searching the jointree, rather
	 * than scanning the rangetable, is that the rangetable may contain RTEs
	 * for rels not actively part of the query, for example views.  We don't
	 * want to make RelOptInfos for them.
	 */
	add_base_rels_to_query(root, (Node *) parse->jointree);
```
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
  - [setup_simple_rel_arrays](../s/setup_simple_rel_arrays.md)
  - [build_simple_rel](../b/build_simple_rel.md)
  - [is_parallel_safe](../i/is_parallel_safe.md)
  - [add_path](../a/add_path.md)
  - [create_group_result_path](../c/create_group_result_path.md)
  - [set_cheapest](../s/set_cheapest.md)
  - [add_base_rels_to_query](../a/add_base_rels_to_query.md)
  - [build_base_rel_tlists](../b/build_base_rel_tlists.md)
  - [find_placeholders_in_jointree](../f/find_placeholders_in_jointree.md)
  - [find_lateral_references](../f/find_lateral_references.md)
  - [deconstruct_jointree](../d/deconstruct_jointree.md)
  - [reconsider_outer_join_clauses](../r/reconsider_outer_join_clauses.md)
  - [generate_base_implied_equalities](../g/generate_base_implied_equalities.md)
  - [fix_placeholder_input_needed_levels](../f/fix_placeholder_input_needed_levels.md)
  - [remove_useless_joins](../r/remove_useless_joins.md)
  - [reduce_unique_semijoins](../r/reduce_unique_semijoins.md)
  - [add_placeholders_to_base_rels](../a/add_placeholders_to_base_rels.md)
  - [create_lateral_join_info](../c/create_lateral_join_info.md)
  - [match_foreign_keys_to_quals](../m/match_foreign_keys_to_quals.md)
  - [extract_restriction_or_clauses](../e/extract_restriction_or_clauses.md)
  - [add_other_rels_to_query](../a/add_other_rels_to_query.md)
  - [distribute_row_identity_vars](../d/distribute_row_identity_vars.md)
  - [make_one_rel](../m/make_one_rel.md)
- Called from (representative examples):
  - [grouping_planner](../g/grouping_planner.md)
  - [build_minmax_path](../b/build_minmax_path.md)

## Notes and Other Information
- The function includes a special optimization path for trivial queries (single RTE_RESULT relations) that bypasses most of the complex planning logic
- Equivalence class merging must be completed before canonical pathkeys can be generated, which is why the callback mechanism is used
- The function performs extensive join optimization including removal of useless outer joins and reduction of unique semijoins
- Placeholder expressions from subquery pullup are carefully managed to ensure proper variable marking at appropriate join levels
- The final validation ensures that at least one usable path exists and that it doesn't require parameters (indicating a planning failure)
- Foreign key relationships are matched to equivalence classes and join quals for additional optimization opportunities
- Appendrel (inheritance/partitioning) expansion is delayed until the end to maximize available information for pruning

## Simplified Source

```c
RelOptInfo *query_planner(PlannerInfo *root,
                         query_pathkeys_callback qp_callback, void *qp_extra)
{
    Query *parse = root->parse;
    List *joinlist;
    RelOptInfo *final_rel;

    // Initialize planner data structures
    root->join_rel_list = NIL;
    root->join_rel_hash = NULL;
    root->join_rel_level = NULL;
    root->join_cur_level = 0;
    root->canon_pathkeys = NIL;
    root->left_join_clauses = NIL;
    root->right_join_clauses = NIL;
    root->full_join_clauses = NIL;
    root->join_info_list = NIL;
    root->placeholder_list = NIL;
    root->placeholder_array = NULL;
    root->placeholder_array_size = 0;
    root->fkey_list = NIL;
    root->initial_rels = NIL;

    // Set up arrays for accessing base relations
    setup_simple_rel_arrays(root);

    // Optimize trivial case: single RTE_RESULT relation
    if (list_length(parse->jointree->fromlist) == 1) {
        Node *jtnode = (Node *) linitial(parse->jointree->fromlist);

        if (IsA(jtnode, RangeTblRef)) {
            int varno = ((RangeTblRef *) jtnode)->rtindex;
            RangeTblEntry *rte = root->simple_rte_array[varno];

            if (rte && rte->rtekind == RTE_RESULT) {
                // Create RelOptInfo for Result relation
                final_rel = build_simple_rel(root, varno, NULL);

                // Check if parallel execution is possible
                if (root->glob->parallelModeOK &&
                    (root->query_level > 1 ||
                     debug_parallel_query != DEBUG_PARALLEL_OFF))
                    final_rel->consider_parallel =
                        is_parallel_safe(root, parse->jointree->quals);

                // Create trivial Result path
                add_path(final_rel, (Path *)
                    create_group_result_path(root, final_rel,
                                           final_rel->reltarget,
                                           (List *) parse->jointree->quals));

                set_cheapest(final_rel);
                root->ec_merging_done = true;
                (*qp_callback) (root, qp_extra);
                return final_rel;
            }
        }
    }

    // Build RelOptInfo nodes for all base relations
    add_base_rels_to_query(root, (Node *) parse->jointree);

    // Analyze targetlist and join tree, build equivalence classes
    build_base_rel_tlists(root, root->processed_tlist);
    find_placeholders_in_jointree(root);
    find_lateral_references(root);
    joinlist = deconstruct_jointree(root);

    // Process outer join clauses with equivalence classes
    reconsider_outer_join_clauses(root);

    // Generate implied equalities from equivalence classes
    generate_base_implied_equalities(root);

    // Compute pathkeys now that equivalence merging is complete
    (*qp_callback) (root, qp_extra);

    // Handle placeholder expressions and join optimization
    fix_placeholder_input_needed_levels(root);
    joinlist = remove_useless_joins(root, joinlist);
    reduce_unique_semijoins(root);
    add_placeholders_to_base_rels(root);
    create_lateral_join_info(root);
    match_foreign_keys_to_quals(root);
    extract_restriction_or_clauses(root);

    // Expand appendrels and distribute row identity variables
    add_other_rels_to_query(root);
    distribute_row_identity_vars(root);

    // Perform the main join planning
    final_rel = make_one_rel(root, joinlist);

    // Validate that we found a usable path
    if (!final_rel || !final_rel->cheapest_total_path ||
        final_rel->cheapest_total_path->param_info != NULL)
        elog(ERROR, "failed to construct the join relation");

    return final_rel;
}
```