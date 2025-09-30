# make_outerjoininfo

## Location
[src/backend/optimizer/plan/initsplan.c:1360-1699](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/initsplan.c#L1360-L1699)

## Overview
Builds a SpecialJoinInfo structure for the current outer join, determining ordering constraints and commutability relationships with other joins in the query tree.

## Definition

```c
union(clause_relids, inner_join_rels),
									right_rels);
```
## Detailed Description
The  function creates and initializes a SpecialJoinInfo structure that captures essential metadata about an outer join operation. This function is critical for the PostgreSQL optimizer's ability to understand join ordering constraints and determine which joins can be safely reordered or commuted.

The function performs several key tasks:
1. Validates join types and enforces restrictions (e.g., FOR UPDATE clauses cannot be applied to nullable sides)
2. Computes minimum left-hand and right-hand relation sets required for the join
3. Analyzes relationships with previously processed outer joins to determine commutability
4. Handles PlaceHolderVar constraints that affect join ordering
5. Applies outer join identity rules to optimize join reordering where possible

The function assumes bottom-up processing, meaning all syntactically lower outer joins have already been processed and are available in root->join_info_list.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning state and optimizer information
- : Bitmap of base+OJ relation IDs syntactically on the outer (left) side of the join
- : Bitmap of base+OJ relation IDs syntactically on the inner (right) side of the join  
- : Bitmap of base+OJ relation IDs participating in inner joins below this outer join
- : Type of join operation (must be LEFT, FULL, SEMI, or ANTI)
- : Range table index of the join RTE (0 for SEMI joins which aren't in the RT list)
- : Join condition for the outer join in implicit-AND format

## Dependencies
- Functions called/Symbols referenced:
  - [compute_semijoin_info](../c/compute_semijoin_info.md)
  - [pull_varnos](../p/pull_varnos.md)
  - [find_nonnullable_rels](../f/find_nonnullable_rels.md)
  - [contain_placeholder_references_to](../c/contain_placeholder_references_to.md)
  - bms_* (various bitmap set operations)
  - [LCS_asString](../L/LCS_asString.md)
- Called from (representative examples):
  - [deconstruct_distribute](../d/deconstruct_distribute.md)

## Notes and Other Information
- The function enforces that FOR UPDATE/SHARE cannot be applied to nullable sides of outer joins, as the executor doesn't support this
- Full joins are treated as optimization barriers - the optimizer cannot associate into or out of them
- The function implements outer join identity rules, particularly identity 3, which allows certain join commutations when strictness conditions are met
- [PlaceHolderVar](../P/PlaceHolderVar.md) handling ensures that expressions are evaluated at appropriate join levels
- Commutability relationships are tracked bidirectionally between SpecialJoinInfo structures
- The returned SpecialJoinInfo should be appended to root->join_info_list by the caller
- Empty min_lefthand or min_righthand sets are expanded to their full respective sides to avoid confusion in later processing

## Simplified Source

```c
static SpecialJoinInfo *make_outerjoininfo(PlannerInfo *root,
                                          Relids left_rels, Relids right_rels,
                                          Relids inner_join_rels,
                                          JoinType jointype, Index ojrelid,
                                          List *clause)
{
    SpecialJoinInfo *sjinfo = makeNode(SpecialJoinInfo);
    Relids clause_relids;
    Relids strict_relids;
    Relids min_lefthand;
    Relids min_righthand;

    // Basic validation and setup
    Assert(jointype != JOIN_INNER && jointype != JOIN_RIGHT);

    // Check FOR UPDATE restrictions on nullable sides
    foreach(l, root->parse->rowMarks) {
        RowMarkClause *rc = (RowMarkClause *) lfirst(l);
        if (bms_is_member(rc->rti, right_rels) ||
            (jointype == JOIN_FULL && bms_is_member(rc->rti, left_rels)))
            ereport(ERROR, /* FOR UPDATE not allowed on nullable side */);
    }

    // Initialize basic fields
    sjinfo->syn_lefthand = left_rels;
    sjinfo->syn_righthand = right_rels;
    sjinfo->jointype = jointype;
    sjinfo->ojrelid = ojrelid;

    compute_semijoin_info(root, sjinfo, clause);

    // Handle full joins simply - they're optimization barriers
    if (jointype == JOIN_FULL) {
        sjinfo->min_lefthand = bms_copy(left_rels);
        sjinfo->min_righthand = bms_copy(right_rels);
        sjinfo->lhs_strict = false;
        return sjinfo;
    }

    // Analyze clause to determine relation dependencies
    clause_relids = pull_varnos(root, (Node *) clause);
    strict_relids = find_nonnullable_rels((Node *) clause);
    sjinfo->lhs_strict = bms_overlap(strict_relids, left_rels);

    // Compute minimum required relations
    min_lefthand = bms_intersect(clause_relids, left_rels);
    min_righthand = bms_int_members(bms_union(clause_relids, inner_join_rels),
                                    right_rels);

    // Check ordering constraints from previous outer joins
    foreach(l, root->join_info_list) {
        SpecialJoinInfo *otherinfo = (SpecialJoinInfo *) lfirst(l);

        // Handle full joins as optimization barriers
        if (otherinfo->jointype == JOIN_FULL) {
            if (bms_overlap(left_rels, otherinfo->syn_lefthand) ||
                bms_overlap(left_rels, otherinfo->syn_righthand)) {
                min_lefthand = bms_add_members(min_lefthand,
                                              otherinfo->syn_lefthand);
                min_lefthand = bms_add_members(min_lefthand,
                                              otherinfo->syn_righthand);
            }
            // Similar logic for right side...
            continue;
        }

        // Apply ordering constraints based on overlap and strictness
        // (Complex logic for commutability analysis simplified...)
    }

    // Handle PlaceHolderVar constraints
    foreach(l, root->placeholder_list) {
        PlaceHolderInfo *phinfo = (PlaceHolderInfo *) lfirst(l);
        Relids ph_syn_level = phinfo->ph_var->phrels;

        if (bms_is_subset(ph_syn_level, right_rels)) {
            min_righthand = bms_add_members(min_righthand, phinfo->ph_eval_at);
        }
    }

    // Ensure non-empty min sets
    if (bms_is_empty(min_lefthand))
        min_lefthand = bms_copy(left_rels);
    if (bms_is_empty(min_righthand))
        min_righthand = bms_copy(right_rels);

    sjinfo->min_lefthand = min_lefthand;
    sjinfo->min_righthand = min_righthand;

    return sjinfo;
}
```