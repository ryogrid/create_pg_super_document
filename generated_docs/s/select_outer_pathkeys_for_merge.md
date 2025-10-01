# select_outer_pathkeys_for_merge

## Location
[src/backend/optimizer/path/pathkeys.c:1639-1834](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/pathkeys.c#L1639-L1834)

## Overview
This function builds a pathkey list representing a possible sort ordering that can be used with given mergeclauses for merge join operations.

## Definition

```c
List *
select_outer_pathkeys_for_merge(PlannerInfo *root,
								List *mergeclauses,
								RelOptInfo *joinrel)
```
## Detailed Description
The function creates an optimal pathkey ordering for the outer relation in a merge join, prioritizing query_pathkeys compatibility and equivalence class popularity. The algorithm works in several phases:

1. **Extract and Score Equivalence Classes**: Collects unique equivalence classes from mergeclauses and scores them based on their potential for future joins (popularity)
2. **Query Pathkeys Matching**: Attempts to match or use a prefix of root->query_pathkeys to avoid additional sorting or enable incremental sorts
3. **Popularity-Based Ordering**: Adds remaining equivalence classes in order of popularity (highest score first)

Key optimization strategies:
- Prefers matching query_pathkeys when all ECs are available
- Uses query_pathkeys prefix when it covers the entire join condition
- Prioritizes "popular" equivalence classes (those with more unmatched members) for better higher-level merge join opportunities

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and context, including query_pathkeys
- : List of RestrictInfos for mergejoin clauses marked with outer_is_left indicators
- : The join relation being constructed, used to determine which equivalence class members are potential future join partners

## Dependencies
- Functions called/Symbols referenced:
  - [update_mergeclause_eclasses](../u/update_mergeclause_eclasses.md)
  - [bms_overlap](../b/bms_overlap.md)
  - [list_copy](../l/list_copy.md)
  - [list_copy_head](../l/list_copy_head.md)
  - [make_canonical_pathkey](../m/make_canonical_pathkey.md)
  - linitial_oid
  - [pathkey_is_redundant](../p/pathkey_is_redundant.md)
  - [EquivalenceClass](../E/EquivalenceClass.md)
  - [EquivalenceMember](../E/EquivalenceMember.md)
  - [PathKey](../P/PathKey.md)
  - BTLessStrategyNumber
- Called from (representative examples):
  - [sort_inner_and_outer](sort_inner_and_outer.md) (src/backend/optimizer/path/joinpath.c:1379)

## Notes and Other Information
- Returns NIL if no mergeclauses are provided
- Assumes a sort is required, so doesn't try to match existing outer relation ordering
- Uses a simple selection sort algorithm for ordering equivalence classes by popularity (acceptable for typically small lists)
- Popularity scoring counts equivalence class members that don't overlap with the current joinrel (potential future join partners)
- The function enables incremental sorting optimizations by trying to match query_pathkeys prefixes
- Creates canonical pathkeys using BTLessStrategyNumber as the default sort strategy

## Simplified Source

```c
List *select_outer_pathkeys_for_merge(PlannerInfo *root,
                                     List *mergeclauses,
                                     RelOptInfo *joinrel)
{
    List *pathkeys = NIL;
    int nClauses = list_length(mergeclauses);

    if (nClauses == 0)
        return NIL;

    // Extract unique equivalence classes and score them
    EquivalenceClass **ecs = (EquivalenceClass **) palloc(nClauses * sizeof(EquivalenceClass *));
    int *scores = (int *) palloc(nClauses * sizeof(int));
    int necs = 0;

    foreach(lc, mergeclauses)
    {
        RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
        EquivalenceClass *oeclass;

        update_mergeclause_eclasses(root, rinfo);

        // Get outer equivalence class
        oeclass = rinfo->outer_is_left ? rinfo->left_ec : rinfo->right_ec;

        // Skip duplicates
        int j;
        for (j = 0; j < necs; j++)
        {
            if (ecs[j] == oeclass)
                break;
        }
        if (j < necs)
            continue;

        // Score based on potential future join partners
        int score = 0;
        foreach(lc2, oeclass->ec_members)
        {
            EquivalenceMember *em = (EquivalenceMember *) lfirst(lc2);
            if (!em->em_is_const && !em->em_is_child &&
                !bms_overlap(em->em_relids, joinrel->relids))
                score++;
        }

        ecs[necs] = oeclass;
        scores[necs] = score;
        necs++;
    }

    // Try to match query_pathkeys for output ordering benefits
    if (root->query_pathkeys)
    {
        int matches = 0;
        foreach(lc, root->query_pathkeys)
        {
            PathKey *query_pathkey = (PathKey *) lfirst(lc);
            EquivalenceClass *query_ec = query_pathkey->pk_eclass;

            // Check if this EC is in our merge clauses
            int j;
            for (j = 0; j < necs; j++)
            {
                if (ecs[j] == query_ec)
                    break;
            }
            if (j >= necs)
                break;  // No match found
            matches++;
        }

        // Use query pathkeys if we have all ECs
        if (lc == NULL)
        {
            pathkeys = list_copy(root->query_pathkeys);
            // Mark these ECs as used
            foreach(lc, root->query_pathkeys)
            {
                PathKey *query_pathkey = (PathKey *) lfirst(lc);
                EquivalenceClass *query_ec = query_pathkey->pk_eclass;
                for (int j = 0; j < necs; j++)
                {
                    if (ecs[j] == query_ec)
                    {
                        scores[j] = -1;
                        break;
                    }
                }
            }
        }
        // Use query pathkeys prefix if it covers all join clauses
        else if (matches == nClauses)
        {
            pathkeys = list_copy_head(root->query_pathkeys, matches);
            pfree(ecs);
            pfree(scores);
            return pathkeys;
        }
    }

    // Add remaining ECs in popularity order
    for (;;)
    {
        int best_j = 0;
        int best_score = scores[0];

        // Find highest scoring unused EC
        for (int j = 1; j < necs; j++)
        {
            if (scores[j] > best_score)
            {
                best_j = j;
                best_score = scores[j];
            }
        }

        if (best_score < 0)
            break;  // All done

        // Create pathkey for this EC
        EquivalenceClass *ec = ecs[best_j];
        scores[best_j] = -1;
        PathKey *pathkey = make_canonical_pathkey(root, ec,
                                                 linitial_oid(ec->ec_opfamilies),
                                                 BTLessStrategyNumber, false);
        pathkeys = lappend(pathkeys, pathkey);
    }

    pfree(ecs);
    pfree(scores);
    return pathkeys;
}
```