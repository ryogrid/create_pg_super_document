# have_dangerous_phv

## Location
[src/backend/optimizer/path/joinrels.c:1305-1332](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/joinrels.c#L1305-L1332)

## Overview
Detects whether any PlaceHolderVars (PHVs) in the query would create dangerous parameterization scenarios for nestloop joins, preventing executor complications with complex parameter expressions.

## Definition

```c
bool
have_dangerous_phv(PlannerInfo *root,
				   Relids outer_relids, Relids inner_params)
```
## Detailed Description
This function identifies potentially problematic PlaceHolderVar scenarios in parameterized nestloop joins. The core issue it addresses is when a PHV's minimum evaluation set includes both the outer relation and some third relation, which would require the PHV expression to be evaluated as a nestloop parameter. Since the executor only handles simple Vars as NestLoopParams, allowing such complex parameter expressions would add significant complexity and overhead.

The function examines all PHVs in the query's placeholder list and applies a three-step safety check:
1. Skip PHVs that cannot be nestloop parameters (not subset of inner_params)
2. Skip PHVs not relevant to this specific join (no overlap with outer_relids)
3. Allow PHVs that can be safely evaluated within the outer relation (subset of outer_relids)

Any PHV that fails these safety checks is considered "dangerous" and will cause the function to reject the proposed join. The function serves as a protective mechanism to ensure that only executor-compatible parameterized plans are generated.

This check is performed in two contexts: during initial join legality assessment in join_is_legal() and again in joinpath.c for each specific nestloop path, since inner paths might have more than minimum parameterization.

## Parameters / Member Variables
- `*root`: Pointer to PlannerInfo containing global planner state and the placeholder list
- `outer_relids`: Bitmapset representing the relations in the outer side of the proposed join
- `inner_params`: Bitmapset representing the parameterization requirements of the inner path
## Dependencies
- Functions called/Symbols referenced:
  -  - Structure containing PlaceHolderVar information
  -  - Tests if one bitmapset is a subset of another
  -  - Tests if two bitmapsets have any common elements

- Called from (representative examples):
  -  - Checks path safety before creating nestloop paths
  -  - Validates join legality during planning
  - Referenced in  header for external visibility

## Notes and Other Information
- Returns true if dangerous PHVs are found (rejecting the join), false if safe to proceed
- The function implements a conservative approach - it doesn't attempt to determine if a risky PHV would actually be used in the inner plan
- This safety check prevents executor complexity by ensuring only simple Var parameters are passed to nestloops
- The check occurs at two different planning stages to catch both minimum and extended parameterizations
- Located in src/backend/optimizer/path/joinrels.c:1305-1332

## Simplified Source

```c
bool have_dangerous_phv(PlannerInfo *root,
                       Relids outer_relids, Relids inner_params) {
    ListCell *lc;

    // Check each placeholder in the global list
    foreach(lc, root->placeholder_list) {
        PlaceHolderInfo *phinfo = (PlaceHolderInfo *) lfirst(lc);

        // Skip if PHV cannot be a nestloop parameter
        if (!bms_is_subset(phinfo->ph_eval_at, inner_params))
            continue;

        // Skip if PHV is not relevant to this join
        if (!bms_overlap(phinfo->ph_eval_at, outer_relids))
            continue;

        // Safe if PHV can be evaluated entirely within outer relation
        if (bms_is_subset(phinfo->ph_eval_at, outer_relids))
            continue;

        // Dangerous PHV found - reject the join
        return true;
    }

    // No dangerous PHVs found - safe to proceed
    return false;
}
```