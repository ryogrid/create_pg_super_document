# reduce_outer_joins_pass2

## Location
[src/backend/optimizer/prep/prepjointree.c:3084-3357](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L3084-L3357)

## Overview
Phase 2 processing function that examines qual clauses and performs actual outer join reductions based on strictness analysis and nullability constraints collected in pass 1.

## Definition
```c
static void reduce_outer_joins_pass2(Node *jtnode,
                                    reduce_outer_joins_pass1_state *state1,
                                    reduce_outer_joins_pass2_state *state2,
                                    PlannerInfo *root,
                                    Relids nonnullable_rels,
                                    List *forced_null_vars)
```

## Detailed Description
This function performs the core optimization logic of the outer join reduction algorithm. It recursively traverses the jointree and applies various transformations based on the analysis of quals and nullability constraints:

**Key Transformations Performed:**
1. **Outer-to-Inner Join Reduction**: Converts LEFT/RIGHT/FULL JOINs to INNER JOINs when upper quals force nullable-side relations to be non-null
2. **FULL Join Partial Reduction**: Reduces FULL JOINs to LEFT or RIGHT JOINs when only one side has nullability constraints
3. **JOIN_RIGHT Normalization**: Converts all RIGHT JOINs to LEFT JOINs by swapping arguments
4. **Anti-Semijoin Detection**: Converts LEFT JOINs to ANTI JOINs when join quals are strict for variables that are forced null by upper constraints

**Constraint Propagation Logic:**
- For INNER/SEMI joins: Merges and propagates both upper and local constraints to child nodes
- For LEFT/ANTI joins: Passes upper constraints to non-nullable side, local constraints to nullable side
- For FULL joins: No constraint propagation to avoid incorrect optimizations

The function maintains state about successfully reduced joins in state2, distinguishing between fully reduced joins (added to inner_reduced) and partially reduced FULL joins (tracked separately for custom nulling relation cleanup).

## Parameters / Member Variables
- `jtnode`: Current jointree node being processed (FromExpr or JoinExpr)
- `state1`: Pass 1 state data containing relation information and outer join indicators
- `state2`: Accumulator for information about successfully reduced joins
- `root`: Top-level planner state containing the parse tree
- `nonnullable_rels`: Set of base relation IDs that are forced non-null by upper quals
- `forced_null_vars`: Multibitmapset of variables that are forced null by upper quals

## Dependencies
- Functions called/Symbols referenced:
  - [find_nonnullable_rels](../f/find_nonnullable_rels.md)
  - [find_forced_null_vars](../f/find_forced_null_vars.md)
  - [find_nonnullable_vars](../f/find_nonnullable_vars.md)
  - [bms_add_members](../b/bms_add_members.md)
  - [mbms_add_members](../m/mbms_add_members.md)
  - [mbms_overlap_sets](../m/mbms_overlap_sets.md)
  - [bms_overlap](../b/bms_overlap.md)
  - [bms_add_member](../b/bms_add_member.md)
  - [bms_free](../b/bms_free.md)
  - [report_reduced_full_join](report_reduced_full_join.md)
  - rt_fetch
  - linitial
  - lsecond
  - forboth
- Called from (representative examples):
  - [reduce_outer_joins](reduce_outer_joins.md) (initial call)
  - [reduce_outer_joins_pass2](reduce_outer_joins_pass2.md) (recursive calls)

## Notes and Other Information
- Static function internal to prepjointree.c, used only within the outer join reduction algorithm
- Requires that jtnode is never NULL or a base relation (RangeTblRef), as these should not appear in subtrees marked as contains_outer
- Updates both the JoinExpr node and corresponding RangeTblEntry when join types are changed
- [Complex](../C/Complex.md) constraint propagation logic ensures that optimizations are applied safely without changing query semantics
- The function handles the intricate interactions between different join types and their nullability semantics
- Proper handling of SEMI/ANTI joins that may be introduced by sublink pullup

## Simplified Source

```c
static void reduce_outer_joins_pass2(Node *jtnode,
                                    reduce_outer_joins_pass1_state *state1,
                                    reduce_outer_joins_pass2_state *state2,
                                    PlannerInfo *root,
                                    Relids nonnullable_rels,
                                    List *forced_null_vars)
{
    // Basic validation - should never reach empty or base nodes
    if (jtnode == NULL || IsA(jtnode, RangeTblRef))
        elog(ERROR, "invalid node type for outer join reduction");

    if (IsA(jtnode, FromExpr))
    {
        FromExpr *f = (FromExpr *) jtnode;

        // Analyze quals to find additional constraints
        Relids pass_nonnullable_rels = find_nonnullable_rels(f->quals);
        pass_nonnullable_rels = bms_add_members(pass_nonnullable_rels, nonnullable_rels);

        List *pass_forced_null_vars = find_forced_null_vars(f->quals);
        pass_forced_null_vars = mbms_add_members(pass_forced_null_vars, forced_null_vars);

        // Recurse into child nodes that contain outer joins
        ListCell *l, *s;
        forboth(l, f->fromlist, s, state1->sub_states)
        {
            reduce_outer_joins_pass1_state *sub_state = lfirst(s);
            if (sub_state->contains_outer)
                reduce_outer_joins_pass2(lfirst(l), sub_state, state2, root,
                                       pass_nonnullable_rels, pass_forced_null_vars);
        }

        bms_free(pass_nonnullable_rels);
    }
    else if (IsA(jtnode, JoinExpr))
    {
        JoinExpr *j = (JoinExpr *) jtnode;
        JoinType jointype = j->jointype;
        reduce_outer_joins_pass1_state *left_state = linitial(state1->sub_states);
        reduce_outer_joins_pass1_state *right_state = lsecond(state1->sub_states);

        // Apply join type reduction based on nullability constraints
        switch (jointype)
        {
            case JOIN_LEFT:
                // Convert to INNER if right side has non-null constraints
                if (bms_overlap(nonnullable_rels, right_state->relids))
                    jointype = JOIN_INNER;
                break;

            case JOIN_RIGHT:
                // Convert to INNER if left side has non-null constraints
                if (bms_overlap(nonnullable_rels, left_state->relids))
                    jointype = JOIN_INNER;
                break;

            case JOIN_FULL:
                // Convert to LEFT/RIGHT/INNER based on constraints
                if (bms_overlap(nonnullable_rels, left_state->relids))
                {
                    if (bms_overlap(nonnullable_rels, right_state->relids))
                        jointype = JOIN_INNER;
                    else
                    {
                        jointype = JOIN_LEFT;
                        report_reduced_full_join(state2, j->rtindex, right_state->relids);
                    }
                }
                else if (bms_overlap(nonnullable_rels, right_state->relids))
                {
                    jointype = JOIN_RIGHT;
                    report_reduced_full_join(state2, j->rtindex, left_state->relids);
                }
                break;
        }

        // Convert RIGHT joins to LEFT joins by swapping arguments
        if (jointype == JOIN_RIGHT)
        {
            Node *tmp = j->larg;
            j->larg = j->rarg;
            j->rarg = tmp;
            jointype = JOIN_LEFT;
            // Swap state pointers too
            right_state = linitial(state1->sub_states);
            left_state = lsecond(state1->sub_states);
        }

        // Check for LEFT to ANTI join conversion
        if (jointype == JOIN_LEFT)
        {
            List *nonnullable_vars = find_nonnullable_vars(j->quals);
            Bitmapset *overlap = mbms_overlap_sets(nonnullable_vars, forced_null_vars);
            if (bms_overlap(overlap, right_state->relids))
                jointype = JOIN_ANTI;
        }

        // Update join type in both JoinExpr and RTE
        if (j->rtindex && jointype != j->jointype)
        {
            RangeTblEntry *rte = rt_fetch(j->rtindex, root->parse->rtable);
            rte->jointype = jointype;
            if (jointype == JOIN_INNER)
                state2->inner_reduced = bms_add_member(state2->inner_reduced, j->rtindex);
        }
        j->jointype = jointype;

        // Recursively process children with appropriate constraint propagation
        if (left_state->contains_outer || right_state->contains_outer)
        {
            Relids local_constraints = find_nonnullable_rels(j->quals);
            List *local_forced_null = find_forced_null_vars(j->quals);

            // Merge constraints appropriately based on join type
            if (jointype == JOIN_INNER || jointype == JOIN_SEMI)
            {
                local_constraints = bms_add_members(local_constraints, nonnullable_rels);
                local_forced_null = mbms_add_members(local_forced_null, forced_null_vars);
            }

            // Recurse with appropriate constraints
            if (left_state->contains_outer)
                reduce_outer_joins_pass2(j->larg, left_state, state2, root,
                                       (jointype == JOIN_INNER || jointype == JOIN_SEMI) ?
                                       local_constraints : nonnullable_rels,
                                       (jointype == JOIN_INNER || jointype == JOIN_SEMI) ?
                                       local_forced_null : forced_null_vars);

            if (right_state->contains_outer)
                reduce_outer_joins_pass2(j->rarg, right_state, state2, root,
                                       local_constraints, local_forced_null);

            bms_free(local_constraints);
        }
    }
    else
        elog(ERROR, "unrecognized node type: %d", (int) nodeTag(jtnode));
}
```