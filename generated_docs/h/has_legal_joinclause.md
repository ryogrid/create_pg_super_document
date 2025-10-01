# has_legal_joinclause

## Location
[src/backend/optimizer/path/joinrels.c:1241-1304](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/joinrels.c#L1241-L1304)

## Overview
Detects whether a specified relation can legally be joined to any other relations using join clauses, serving as a heuristic to optimize join planning by identifying relations that have valid join opportunities.

## Definition

```c
union */
			joinrelids = bms_union(rel->relids, rel2->relids);
```
## Detailed Description
This function examines whether a given relation can participate in legal joins with other relations in the current planning context. It iterates through all relations in the  list and checks if there are relevant join clauses between the target relation and each candidate relation. For each potential join, it verifies that the join is legally permissible according to PostgreSQL's join ordering constraints.

The function implements a conservative heuristic approach - it only considers joins to single other relations (not to join results) to avoid the computational complexity of proving that complex join combinations can be legally formed. This may occasionally produce false negatives (returning false when legal joins exist), but this trades off some accuracy for significantly better planning performance.

The function is specifically designed to work within the context of sub-joinlist planning, where clauseless joins within  might be forced even when join clauses exist linking to other parts of the query.

## Parameters / Member Variables
- : Pointer to the PlannerInfo structure containing global planner state and context
- : The RelOptInfo representing the relation being tested for legal join opportunities

## Dependencies
- Functions called/Symbols referenced:
  -  - Tests if two bitmapsets have overlapping bits
  -  - Checks if join clauses exist between two relations
  -  - Creates union of two bitmapsets
  -  - Validates if a specific join is legally permissible
  -  - Deallocates bitmapset memory
  -  - Structure type for special join information

- Called from (representative examples):
  -  - Uses this function to determine join ordering constraints

## Notes and Other Information
- This function is static and only used within the joinrels.c module
- The heuristic nature means it may miss some valid join opportunities in complex scenarios
- Memory management is carefully handled with  calls to prevent leaks
- The function specifically excludes relations that are already part of the input relation (using  check)
- Located in src/backend/optimizer/path/joinrels.c:1241-1304

## Simplified Source

```c
static bool has_legal_joinclause(PlannerInfo *root, RelOptInfo *rel) {
    ListCell *lc;

    // Check each relation in initial_rels for potential joins
    foreach(lc, root->initial_rels) {
        RelOptInfo *rel2 = (RelOptInfo *) lfirst(lc);

        // Skip relations already included in rel
        if (bms_overlap(rel->relids, rel2->relids))
            continue;

        // Check for relevant join clauses
        if (have_relevant_joinclause(root, rel, rel2)) {
            Relids joinrelids = bms_union(rel->relids, rel2->relids);
            SpecialJoinInfo *sjinfo;
            bool reversed;

            // Verify the join is legal
            if (join_is_legal(root, rel, rel2, joinrelids, &sjinfo, &reversed)) {
                bms_free(joinrelids);
                return true;
            }

            bms_free(joinrelids);
        }
    }

    return false;
}
```