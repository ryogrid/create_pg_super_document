# check_redundant_nullability_qual

## Location
[src/backend/optimizer/plan/initsplan.c:2584-2628](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/initsplan.c#L2584-L2628)

## Overview
Checks whether an IS NULL qualification is redundant with a lower anti-join, allowing the optimizer to suppress unnecessary null checks.

## Definition
```c
static bool check_redundant_nullability_qual(PlannerInfo *root, Node *clause)
```

## Detailed Description
This function performs a specialized optimization by detecting redundant IS NULL qualifications that are guaranteed to be true due to anti-join semantics. When a variable is forced to NULL by an anti-join operation, any subsequent IS NULL test on that variable is redundant and can be safely removed from the query plan.

The function operates by:
1. **IS NULL Detection**: Uses find_forced_null_var() to identify if the clause is an IS NULL test and extracts the target variable
2. **Nulling Analysis**: Examines the variable's varnullingrels to determine if it's nulled by any join operations
3. **Anti-join Search**: Searches through the join_info_list to find anti-joins that null the variable
4. **Redundancy Determination**: Returns true if a matching anti-join is found, indicating the IS NULL test is redundant

The primary motivation is to avoid generating bogus selectivity estimates for conditions that are guaranteed to be true, rather than just saving execution cycles.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planner state and join information
- `clause`: The qualification clause to check for redundancy

## Dependencies
- Functions called/Symbols referenced:
  - [find_forced_null_var](../f/find_forced_null_var.md)
  - [SpecialJoinInfo](../S/SpecialJoinInfo.md)
  - JOIN_ANTI
  - [bms_is_member](../b/bms_is_member.md)
- Called from (representative examples):
  - [distribute_qual_to_rels](../d/distribute_qual_to_rels.md)

## Notes and Other Information
This function is part of PostgreSQL's query optimization strategy for handling anti-joins and null semantics. Key implementation details:
- **Anti-join Specific**: Only checks for redundancy with JOIN_ANTI operations, not other join types
- **Nulling Relation Tracking**: Relies on the varnullingrels bitmap to track which joins force variables to NULL
- **Selectivity Optimization**: Primary goal is preventing inaccurate selectivity estimates rather than runtime performance
- **Conservative Approach**: Only removes qualifications when redundancy is definitively proven
- **Special Case Handling**: Accounts for anti-joins converted from semi-joins where ojrelid might be zero

The function plays a crucial role in maintaining accurate cardinality estimates for the cost-based optimizer, especially in queries with complex anti-join patterns.

## Simplified Source

```c
static bool check_redundant_nullability_qual(PlannerInfo *root, Node *clause) {
    Var *forced_null_var;
    ListCell *lc;

    // Check if this is an IS NULL clause and get the variable
    forced_null_var = find_forced_null_var(clause);
    if (forced_null_var == NULL)
        return false;

    // If variable isn't nulled by any join, no redundancy possible
    if (forced_null_var->varnullingrels == NULL)
        return false;

    // Search for anti-joins that null this variable
    foreach(lc, root->join_info_list) {
        SpecialJoinInfo *sjinfo = (SpecialJoinInfo *) lfirst(lc);

        // Check if this anti-join nulls our variable
        if (sjinfo->jointype == JOIN_ANTI &&
            sjinfo->ojrelid != 0 &&
            bms_is_member(sjinfo->ojrelid, forced_null_var->varnullingrels)) {
            return true; // Redundant IS NULL found
        }
    }

    return false;
}
```