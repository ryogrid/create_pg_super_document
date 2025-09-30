# add_join_clause_to_rels

## Location
[src/backend/optimizer/util/joininfo.c:98-160](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/joininfo.c#L98-L160)

## Overview
Adds a join restriction clause to the joininfo list of each relation that participates in the join, enabling the query optimizer to track which relations can be joined together.

## Definition
```c
void add_join_clause_to_rels(PlannerInfo *root, RestrictInfo *restrictinfo, Relids join_relids)
```

## Detailed Description
This function distributes a join restriction clause to all participating base relations by adding it to their joininfo lists. The same RestrictInfo node is shared across all lists to enable caching of information about the restriction clause, though care must be taken that cached information is context-independent.

The function performs several optimizations:
1. Skips adding clauses that are always true (trivial conditions)
2. Converts always-false clauses to constant-FALSE while preserving the rinfo_serial to maintain consistency for identical conditions
3. Only adds clauses to base relations, skipping join relations

The preservation of rinfo_serial numbers is critical for ensuring that RestrictInfos representing the "same" qualifier condition receive identical serial numbers, which is essential for proper handling in functions like deconstruct_distribute_oj_quals.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planning information
- `restrictinfo`: RestrictInfo node describing the join clause to be distributed
- `join_relids`: Bitmap set of relation IDs participating in the join clause

## Dependencies
- Functions called/Symbols referenced:
  - [restriction_is_always_true](../r/restriction_is_always_true.md)
  - [restriction_is_always_false](../r/restriction_is_always_false.md)  
  - [make_restrictinfo](../m/make_restrictinfo.md)
  - [makeBoolConst](../m/makeBoolConst.md)
  - [bms_next_member](../b/bms_next_member.md)
  - [find_base_rel_ignore_join](../f/find_base_rel_ignore_join.md)
- Called from (representative examples):
  - [distribute_restrictinfo_to_rels](../d/distribute_restrictinfo_to_rels.md)

## Notes and Other Information
- The same RestrictInfo node is shared across multiple joininfo lists for efficiency
- Serial number preservation ensures consistency in restriction clause identification
- Only base relations receive the join clauses, not derived join relations
- Always-false conditions are converted to constant FALSE expressions for optimization
- Located in src/backend/optimizer/util/joininfo.c:98-160

## Simplified Source

```c
void
add_join_clause_to_rels(PlannerInfo *root, RestrictInfo *restrictinfo, Relids join_relids)
{
    int cur_relid;

    // Skip trivially true clauses
    if (restriction_is_always_true(root, restrictinfo))
        return;

    // Convert trivially false clauses to constant FALSE
    if (restriction_is_always_false(root, restrictinfo))
    {
        int save_rinfo_serial = restrictinfo->rinfo_serial;
        int save_last_rinfo_serial = root->last_rinfo_serial;

        // Create FALSE constant while preserving serial numbers
        restrictinfo = make_restrictinfo(root,
                                       (Expr *) makeBoolConst(false, false),
                                       restrictinfo->is_pushed_down,
                                       restrictinfo->has_clone,
                                       restrictinfo->is_clone,
                                       restrictinfo->pseudoconstant,
                                       0, /* security_level */
                                       restrictinfo->required_relids,
                                       restrictinfo->incompatible_relids,
                                       restrictinfo->outer_relids);
        restrictinfo->rinfo_serial = save_rinfo_serial;
        root->last_rinfo_serial = save_last_rinfo_serial;
    }

    // Add clause to each participating base relation
    cur_relid = -1;
    while ((cur_relid = bms_next_member(join_relids, cur_relid)) >= 0)
    {
        RelOptInfo *rel = find_base_rel_ignore_join(root, cur_relid);

        // Only add to base relations, skip join relations
        if (rel == NULL)
            continue;
        rel->joininfo = lappend(rel->joininfo, restrictinfo);
    }
}
```