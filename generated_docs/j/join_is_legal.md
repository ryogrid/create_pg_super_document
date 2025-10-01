# join_is_legal

## Location
[src/backend/optimizer/path/joinrels.c:350-669](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/joinrels.c#L350-L669)

## Overview
Determines whether a proposed join between two relations is legal given the query's join order constraints and special join requirements, and identifies the appropriate join type and parameters.

## Definition

```c
static bool
join_is_legal(PlannerInfo *root, RelOptInfo *rel1, RelOptInfo *rel2,
			  Relids joinrelids,
			  SpecialJoinInfo **sjinfo_p, bool *reversed_p)
```
## Detailed Description
The  function is a critical component of PostgreSQL's join planning that validates whether two relations can be legally joined according to the query's constraints. It handles complex join order restrictions arising from:

1. **Special joins** (LEFT, RIGHT, FULL, SEMI, ANTI) with their specific ordering requirements
2. **LATERAL references** that impose nestloop join requirements
3. **Join order restrictions** that prevent certain join combinations

The function performs comprehensive analysis including:
- Scanning the join info list for relevant SpecialJoinInfo nodes
- Checking for proper left/right hand side containment in special joins
- Handling semijoin unique-ification scenarios for optimization opportunities
- Validating LATERAL reference constraints and nestloop feasibility
- Ensuring joins can be parameterized correctly without creating impossible plans

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context
- : First relation to be joined
- : Second relation to be joined  
- : Union of relids from both relations (pre-computed for efficiency)
- : Output parameter set to the matching SpecialJoinInfo node (NULL for inner joins)
- : Output parameter indicating if relations need to be swapped to match the SpecialJoinInfo

## Dependencies
- Functions called/Symbols referenced:
  - [bms_overlap](../b/bms_overlap.md)
  - [bms_is_subset](../b/bms_is_subset.md)
  - [bms_equal](../b/bms_equal.md)
  - [create_unique_path](../c/create_unique_path.md)
  - [have_dangerous_phv](../h/have_dangerous_phv.md)
  - min_join_parameterization
  - [bms_copy](../b/bms_copy.md)
  - [bms_add_members](../b/bms_add_members.md)
- Called from (representative examples):
  - [make_join_rel](../m/make_join_rel.md)
  - [has_legal_joinclause](../h/has_legal_joinclause.md)

## Notes and Other Information
- Returns false if the join violates any special join constraints or LATERAL reference requirements
- Handles complex cases like semijoin unique-ification where RHS relations can be made unique to enable more flexible join ordering
- Includes sophisticated logic for associating joins into special join RHS when previous constraint violations were deemed acceptable
- LATERAL reference handling ensures nestloop implementation feasibility and prevents dangerous parameterized hash variable scenarios
- The function's validation is essential for preventing the generation of invalid execution plans that could produce incorrect query results
- Static function scope restricts usage to within the same source file

## Simplified Source

```c
static bool join_is_legal(PlannerInfo *root, RelOptInfo *rel1, RelOptInfo *rel2,
                         Relids joinrelids, SpecialJoinInfo **sjinfo_p, bool *reversed_p) {
    SpecialJoinInfo *match_sjinfo = NULL;
    bool reversed = false;
    bool must_be_leftjoin = false;
    ListCell *l;

    // Initialize output parameters
    *sjinfo_p = NULL;
    *reversed_p = false;

    // Check special join constraints
    foreach(l, root->join_info_list) {
        SpecialJoinInfo *sjinfo = (SpecialJoinInfo *) lfirst(l);

        // Skip if not relevant to this join
        if (!bms_overlap(sjinfo->min_righthand, joinrelids) ||
            bms_is_subset(joinrelids, sjinfo->min_righthand))
            continue;

        // Skip if special join already done within input relations
        if ((bms_is_subset(sjinfo->min_lefthand, rel1->relids) &&
             bms_is_subset(sjinfo->min_righthand, rel1->relids)) ||
            (bms_is_subset(sjinfo->min_lefthand, rel2->relids) &&
             bms_is_subset(sjinfo->min_righthand, rel2->relids)))
            continue;

        // Check if this join matches the special join pattern
        if (bms_is_subset(sjinfo->min_lefthand, rel1->relids) &&
            bms_is_subset(sjinfo->min_righthand, rel2->relids)) {
            if (match_sjinfo) return false; // Multiple matches invalid
            match_sjinfo = sjinfo;
            reversed = false;
        }
        else if (bms_is_subset(sjinfo->min_lefthand, rel2->relids) &&
                 bms_is_subset(sjinfo->min_righthand, rel1->relids)) {
            if (match_sjinfo) return false; // Multiple matches invalid
            match_sjinfo = sjinfo;
            reversed = true;
        }
        // Handle semijoin unique-ification cases
        else if (sjinfo->jointype == JOIN_SEMI &&
                 ((bms_equal(sjinfo->syn_righthand, rel2->relids) &&
                   create_unique_path(root, rel2, rel2->cheapest_total_path, sjinfo) != NULL) ||
                  (bms_equal(sjinfo->syn_righthand, rel1->relids) &&
                   create_unique_path(root, rel1, rel1->cheapest_total_path, sjinfo) != NULL))) {
            if (match_sjinfo) return false;
            match_sjinfo = sjinfo;
            reversed = bms_equal(sjinfo->syn_righthand, rel1->relids);
        }
        else {
            // Check if join can associate into special join RHS
            if (bms_overlap(rel1->relids, sjinfo->min_righthand) &&
                bms_overlap(rel2->relids, sjinfo->min_righthand))
                continue; // Assume valid previous violation

            // Must be LEFT join to associate into RHS
            if (sjinfo->jointype != JOIN_LEFT ||
                bms_overlap(joinrelids, sjinfo->min_lefthand))
                return false;

            must_be_leftjoin = true;
        }
    }

    // Validate that required LEFT join constraint is satisfied
    if (must_be_leftjoin &&
        (match_sjinfo == NULL ||
         match_sjinfo->jointype != JOIN_LEFT ||
         !match_sjinfo->lhs_strict))
        return false;

    // Check LATERAL reference constraints
    if (root->hasLateralRTEs) {
        bool lateral_fwd = bms_overlap(rel1->relids, rel2->lateral_relids);
        bool lateral_rev = bms_overlap(rel2->relids, rel1->lateral_relids);

        // Cannot have lateral references in both directions
        if (lateral_fwd && lateral_rev)
            return false;

        // Check nestloop implementation feasibility
        if (lateral_fwd || lateral_rev) {
            if (match_sjinfo && (match_sjinfo->jointype == JOIN_FULL))
                return false;

            // Verify direct references and safety
            if (lateral_fwd) {
                if (!bms_overlap(rel1->relids, rel2->direct_lateral_relids) ||
                    have_dangerous_phv(root, rel1->relids, rel2->lateral_relids))
                    return false;
            } else {
                if (!bms_overlap(rel2->relids, rel1->direct_lateral_relids) ||
                    have_dangerous_phv(root, rel2->relids, rel1->lateral_relids))
                    return false;
            }
        }

        // Check for impossible parameterization scenarios
        Relids join_lateral_rels = min_join_parameterization(root, joinrelids, rel1, rel2);
        if (join_lateral_rels) {
            // Complex logic to verify join compatibility with outer joins
            // (simplified here for brevity)
        }
    }

    // Join is legal
    *sjinfo_p = match_sjinfo;
    *reversed_p = reversed;
    return true;
}
```