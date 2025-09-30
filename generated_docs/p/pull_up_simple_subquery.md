# pull_up_simple_subquery

## Location
[src/backend/optimizer/prep/prepjointree.c:1123-1468](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L1123-L1468)

## Overview
Performs the complex transformation of pulling up a simple subquery into the parent query by merging range tables, adjusting variable references, and handling various semantic complications.

## Definition

```c
structure for this subquery.
	 *
	 * NOTE: the next few steps should match the first processing in
	 * subquery_planner().  Can we refactor to avoid code duplication, or
	 * would that just make things uglier?
	 */
	subroot = makeNode(PlannerInfo);
```
## Detailed Description
This function implements the core mechanics of subquery pull-up transformation. It takes a subquery that has been determined to be "simple" (no aggregation, DISTINCT, etc.) and physically merges it into the parent query by:

1. **Subquery Preprocessing**: Creates a PlannerInfo for the subquery and recursively processes it (pull-up SubLinks, preprocess functions, recursively pull up nested subqueries).

2. **Variable Offset Adjustment**: Adjusts variable numbers in the subquery to account for appending its range table to the parent's range table.

3. **Variable Level Adjustment**: Updates variable sublevel references since upper-level variables are now one level closer to their parent.

4. **Variable Replacement**: Replaces all references to the subquery in the parent query with references to the subquery's target list items, potentially wrapping them in PlaceHolderVars.

5. **Range Table Merging**: Combines the subquery's range table and related metadata with the parent query.

6. **Lateral Reference Handling**: Propagates LATERAL markers to child RTEs that might contain lateral cross-references.

7. **Cleanup**: Handles various housekeeping tasks like updating flags and combining lists.

The function includes extensive safety checks and can abort the pull-up if conditions change during processing.

## Parameters / Member Variables
- : PlannerInfo structure for the parent query
- : RangeTblRef node representing the subquery in the jointree
- : RangeTblEntry for the subquery being pulled up
- : Reference to lowest containing outer join, or NULL
- : Reference to containing append relation, or NULL

## Dependencies
- Functions called/Symbols referenced:
  -  - Creates modifiable copy of subquery
  -  - Handles empty FROM clauses
  -  - Processes SubLinks within subquery
  -  - Preprocesses function RTEs in subquery
  -  - Recursively processes subquery's subqueries
  -  - Re-validates subquery simplicity
  -  - Flattens join alias variables
  -  - Adjusts variable numbers
  -  - Adjusts variable sublevels
  -  - Performs variable replacement
  -  - Merges range tables
  -  - Gets relation IDs from jointree
  -  - Updates PlaceHolderVar relids
  -  - Fixes AppendRelInfo relids

- Called from (representative examples):
  -  - During recursive subquery processing

## Notes and Other Information
- The function creates a complete PlannerInfo structure for the subquery, duplicating setup from
- [Variable](../V/Variable.md) replacement requires careful handling of PlaceHolderVars, especially for appendrel members and queries with grouping sets
- LATERAL subqueries require special handling to propagate lateral markers to child RTEs
- The function performs extensive validation and can abort pull-up if the subquery becomes non-simple during processing
- [Range](../R/Range.md) table combination preserves all metadata including row marks and permission info
- The original subquery is nulled out in the RTE to avoid waste when the query is later pulled up again
- Returns either the subquery's jointree or a single member if the FromExpr is degenerate

## Simplified Source

```c
static Node *
pull_up_simple_subquery(PlannerInfo *root, Node *jtnode, RangeTblEntry *rte,
                        JoinExpr *lowest_outer_join,
                        AppendRelInfo *containing_appendrel)
{
    Query *parse = root->parse;
    int varno = ((RangeTblRef *) jtnode)->rtindex;
    Query *subquery = copyObject(rte->subquery);
    PlannerInfo *subroot;
    int rtoffset;
    pullup_replace_vars_context rvcontext;

    // Create PlannerInfo for subquery and process it
    subroot = makeNode(PlannerInfo);
    subroot->parse = subquery;
    subroot->glob = root->glob;
    // ... initialize other subroot fields ...

    replace_empty_jointree(subquery);

    if (subquery->hasSubLinks)
        pull_up_sublinks(subroot);

    preprocess_function_rtes(subroot);
    pull_up_subqueries(subroot);

    // Re-validate simplicity after processing
    if (!is_simple_subquery(root, subquery, rte, lowest_outer_join) ||
        (containing_appendrel != NULL && !is_safe_append_member(subquery)))
        return jtnode;

    // Flatten join alias vars and adjust variable references
    subquery->targetList = flatten_join_alias_vars(subroot, subroot->parse,
                                                   subquery->targetList);

    rtoffset = list_length(parse->rtable);
    OffsetVarNodes((Node *) subquery, rtoffset, 0);
    IncrementVarSublevelsUp((Node *) subquery, -1, 1);

    // Set up variable replacement context
    rvcontext.root = root;
    rvcontext.targetlist = subquery->targetList;
    rvcontext.target_rte = rte;
    rvcontext.varno = varno;
    rvcontext.wrap_non_vars = (containing_appendrel != NULL || parse->groupingSets);

    // Replace variables throughout parent query
    perform_pullup_replace_vars(root, &rvcontext, containing_appendrel);

    // Handle LATERAL propagation
    if (rte->lateral) {
        // Propagate LATERAL to child RTEs that might have lateral refs
        foreach(lc, subquery->rtable) {
            RangeTblEntry *child_rte = lfirst(lc);
            if (child_rte->rtekind != RTE_JOIN && child_rte->rtekind != RTE_CTE)
                child_rte->lateral = true;
        }
    }

    // Merge range tables and update metadata
    CombineRangeTables(&parse->rtable, &parse->rteperminfos,
                       subquery->rtable, subquery->rteperminfos);

    // Update flags and clean up
    parse->hasSubLinks |= subquery->hasSubLinks;
    parse->hasRowSecurity |= subquery->hasRowSecurity;
    rte->subquery = NULL;

    // Return appropriate jointree node
    if (subquery->jointree->quals == NULL &&
        list_length(subquery->jointree->fromlist) == 1)
        return linitial(subquery->jointree->fromlist);

    return (Node *) subquery->jointree;
}
```