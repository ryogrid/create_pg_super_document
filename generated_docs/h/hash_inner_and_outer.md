# hash_inner_and_outer

## Location
[src/backend/optimizer/path/joinpath.c:2093-2346](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/joinpath.c#L2093-L2346)

## Overview
Creates hash join paths by explicitly hashing both outer and inner keys of available hash clauses, exploring various combinations of outer and inner paths to find optimal hash join strategies.

## Definition
```c
static void hash_inner_and_outer(PlannerInfo *root,
                                RelOptInfo *joinrel,
                                RelOptInfo *outerrel,
                                RelOptInfo *innerrel,
                                JoinType jointype,
                                JoinPathExtraData *extra)
```

## Detailed Description
This function is responsible for generating hash join paths by systematically examining all possible combinations of outer and inner relation paths. It first identifies usable hash clauses from the join's restriction list, ensuring they are suitable for hash joining and properly match the outer and inner relations.

The function handles various join types differently, applying uniqueness constraints when necessary for JOIN_UNIQUE_OUTER and JOIN_UNIQUE_INNER. It considers both cheapest-total-cost and cheapest-startup-cost paths for comprehensive optimization.

For parallel execution, the function explores partial hash join paths when the join relation is parallel-safe, including shared hash table construction using parallel hash joins when both outer and inner relations have partial paths available.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planner state and context
- `joinrel`: The target join relation for which paths are being generated
- `outerrel`: The outer relation in the join operation
- `innerrel`: The inner relation in the join operation
- `jointype`: The type of join operation (INNER, OUTER, UNIQUE variants, etc.)
- `extra`: Additional join-specific data including restriction clauses

## Dependencies
- Functions called/Symbols referenced:
  - JoinType (enum type)
  - [JoinPathExtraData](../J/JoinPathExtraData.md) (struct type)
  - IS_OUTER_JOIN
  - RINFO_IS_PUSHED_DOWN
  - [clause_sides_match_join](../c/clause_sides_match_join.md)
  - PATH_PARAM_BY_REL
  - JOIN_UNIQUE_OUTER, JOIN_UNIQUE_INNER, JOIN_INNER, JOIN_FULL, JOIN_RIGHT, JOIN_RIGHT_ANTI (enum values)
  - [create_unique_path](../c/create_unique_path.md)
  - [try_hashjoin_path](../t/try_hashjoin_path.md)
  - [try_partial_hashjoin_path](../t/try_partial_hashjoin_path.md)
  - bms_is_empty
  - [get_cheapest_parallel_safe_total_inner](../g/get_cheapest_parallel_safe_total_inner.md)
- Called from (representative examples):
  - [add_paths_to_joinrel](../a/add_paths_to_joinrel.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the joinpath.c compilation unit
- The function filters hash clauses based on join type - outer joins only use their own clauses while inner joins are less restrictive
- [Hash](../H/Hash.md) joins require that neither path is parameterized by the other relation
- For JOIN_UNIQUE_OUTER and JOIN_UNIQUE_INNER, the function applies uniqueness through create_unique_path
- Parallel hash joins are only considered when enable_parallel_hash is true and certain join type restrictions are met
- The function cannot handle certain join types (FULL, RIGHT, RIGHT_ANTI) with parallelism due to match bit distribution requirements
- Parameterized paths are explored systematically, avoiding combinations where paths are parameterized by their counterpart relations

## Simplified Source

```c
static void hash_inner_and_outer(PlannerInfo *root,
                                RelOptInfo *joinrel,
                                RelOptInfo *outerrel,
                                RelOptInfo *innerrel,
                                JoinType jointype,
                                JoinPathExtraData *extra)
{
    JoinType save_jointype = jointype;
    bool isouterjoin = IS_OUTER_JOIN(jointype);
    List *hashclauses = NIL;
    ListCell *l;

    // Collect usable hash clauses from restriction list
    foreach(l, extra->restrictlist)
    {
        RestrictInfo *restrictinfo = (RestrictInfo *) lfirst(l);

        // Skip pushed-down clauses for outer joins
        if (isouterjoin && RINFO_IS_PUSHED_DOWN(restrictinfo, joinrel->relids))
            continue;

        // Must be hashjoinable
        if (!restrictinfo->can_join || restrictinfo->hashjoinoperator == InvalidOid)
            continue;

        // Check if clause matches join sides
        if (!clause_sides_match_join(restrictinfo, outerrel, innerrel))
            continue;

        hashclauses = lappend(hashclauses, restrictinfo);
    }

    if (hashclauses)
    {
        // Get cheapest paths
        Path *cheapest_startup_outer = outerrel->cheapest_startup_path;
        Path *cheapest_total_outer = outerrel->cheapest_total_path;
        Path *cheapest_total_inner = innerrel->cheapest_total_path;

        // Can't use hashjoin if paths are parameterized by other rel
        if (PATH_PARAM_BY_REL(cheapest_total_outer, innerrel) ||
            PATH_PARAM_BY_REL(cheapest_total_inner, outerrel))
            return;

        // Handle unique join variants
        if (jointype == JOIN_UNIQUE_OUTER)
        {
            cheapest_total_outer = create_unique_path(root, outerrel,
                                                    cheapest_total_outer, extra->sjinfo);
            jointype = JOIN_INNER;
            try_hashjoin_path(root, joinrel, cheapest_total_outer,
                            cheapest_total_inner, hashclauses, jointype, extra);
        }
        else if (jointype == JOIN_UNIQUE_INNER)
        {
            cheapest_total_inner = create_unique_path(root, innerrel,
                                                    cheapest_total_inner, extra->sjinfo);
            jointype = JOIN_INNER;
            try_hashjoin_path(root, joinrel, cheapest_total_outer,
                            cheapest_total_inner, hashclauses, jointype, extra);

            // Try startup outer with unique inner
            if (cheapest_startup_outer && cheapest_startup_outer != cheapest_total_outer)
                try_hashjoin_path(root, joinrel, cheapest_startup_outer,
                                cheapest_total_inner, hashclauses, jointype, extra);
        }
        else
        {
            // Regular joins: try startup outer with total inner
            if (cheapest_startup_outer)
                try_hashjoin_path(root, joinrel, cheapest_startup_outer,
                                cheapest_total_inner, hashclauses, jointype, extra);

            // Try all parameterized path combinations
            foreach(lc1, outerrel->cheapest_parameterized_paths)
            {
                Path *outerpath = (Path *) lfirst(lc1);

                if (PATH_PARAM_BY_REL(outerpath, innerrel))
                    continue;

                foreach(lc2, innerrel->cheapest_parameterized_paths)
                {
                    Path *innerpath = (Path *) lfirst(lc2);

                    if (PATH_PARAM_BY_REL(innerpath, outerrel))
                        continue;

                    if (outerpath == cheapest_startup_outer &&
                        innerpath == cheapest_total_inner)
                        continue; // already tried

                    try_hashjoin_path(root, joinrel, outerpath, innerpath,
                                    hashclauses, jointype, extra);
                }
            }
        }

        // Consider parallel hash joins if joinrel is parallel-safe
        if (joinrel->consider_parallel &&
            save_jointype != JOIN_UNIQUE_OUTER &&
            outerrel->partial_pathlist != NIL &&
            bms_is_empty(joinrel->lateral_relids))
        {
            Path *cheapest_partial_outer = linitial(outerrel->partial_pathlist);

            // Try parallel hash with partial inner if available
            if (innerrel->partial_pathlist != NIL &&
                save_jointype != JOIN_UNIQUE_INNER &&
                enable_parallel_hash)
            {
                Path *cheapest_partial_inner = linitial(innerrel->partial_pathlist);
                try_partial_hashjoin_path(root, joinrel,
                                        cheapest_partial_outer, cheapest_partial_inner,
                                        hashclauses, jointype, extra, true);
            }

            // Try partial outer with safe total inner
            Path *cheapest_safe_inner = NULL;
            if (save_jointype == JOIN_FULL || save_jointype == JOIN_RIGHT ||
                save_jointype == JOIN_RIGHT_ANTI)
                cheapest_safe_inner = NULL; // Can't parallelize these
            else if (cheapest_total_inner->parallel_safe)
                cheapest_safe_inner = cheapest_total_inner;
            else if (save_jointype != JOIN_UNIQUE_INNER)
                cheapest_safe_inner = get_cheapest_parallel_safe_total_inner(innerrel->pathlist);

            if (cheapest_safe_inner)
                try_partial_hashjoin_path(root, joinrel,
                                        cheapest_partial_outer, cheapest_safe_inner,
                                        hashclauses, jointype, extra, false);
        }
    }
}
```