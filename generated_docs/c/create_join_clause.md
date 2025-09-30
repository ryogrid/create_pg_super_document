# create_join_clause

## Location
[src/backend/optimizer/path/equivclass.c:1808-1991](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/equivclass.c#L1808-L1991)

## Overview
Creates or retrieves a RestrictInfo clause that compares two EquivalenceMembers using a specified operator, handling both existing and newly-derived join conditions.

## Definition
```c
static RestrictInfo *create_join_clause(PlannerInfo *root, EquivalenceClass *ec, Oid opno, EquivalenceMember *leftem, EquivalenceMember *rightem, EquivalenceClass *parent_ec)
```

## Detailed Description
This function creates join clauses by finding or constructing RestrictInfo structures that compare two members of an EquivalenceClass. It first searches through existing source clauses (ec_sources) and previously-derived clauses (ec_derives) to avoid creating duplicate clauses. If no existing clause is found, it constructs a new one using build_implied_join_equality.

The function handles complex scenarios involving child relations from appendrel expansions, ensuring that clause_relids are correctly set and parent-child relationships are maintained through rinfo_serial propagation. It also manages memory allocation by switching to the planner context for reusability, particularly important in GEQO planning.

## Parameters / Member Variables
- `root`: Pointer to the PlannerInfo containing global planning state
- `ec`: The EquivalenceClass containing the members to be compared
- `opno`: OID of the comparison operator to use
- `leftem`: Left-side EquivalenceMember in the comparison
- `rightem`: Right-side EquivalenceMember in the comparison  
- `parent_ec`: Parent EquivalenceClass (equals ec for join clauses, NULL for restriction clauses)

## Dependencies
- Functions called/Symbols referenced:
  - [build_implied_join_equality](../b/build_implied_join_equality.md) (to construct new RestrictInfo structures)
  - [create_join_clause](create_join_clause.md) (recursive call for parent-child relationships)
  - [bms_union](../b/bms_union.md) (to combine relation bitmaps)
  - [bms_add_members](../b/bms_add_members.md) (to add relations to clause_relids)
  - [EquivalenceClass](../E/EquivalenceClass.md), EquivalenceMember (struct types)
- Called from (representative examples):
  - [generate_join_implied_equalities_normal](../g/generate_join_implied_equalities_normal.md)
  - [generate_implied_equalities_for_column](../g/generate_implied_equalities_for_column.md)
  - [create_join_clause](create_join_clause.md) (recursive self-call)

## Notes and Other Information
- Returns existing RestrictInfo if a matching clause is found, otherwise creates a new one
- Handles commutative operators by checking both left-right and right-left operand arrangements
- Manages parent-child relationships for appendrel expansions by recursively creating parent clauses
- Uses planner memory context to ensure clause reusability across different planning phases
- The parent_ec parameter distinguishes between join clauses and restriction clauses for the same EM pair
- Automatically sets left_ec and right_ec to the provided EquivalenceClass to avoid additional lookups

## Simplified Source

```c
static RestrictInfo *
create_join_clause(PlannerInfo *root, EquivalenceClass *ec, Oid opno,
                   EquivalenceMember *leftem, EquivalenceMember *rightem,
                   EquivalenceClass *parent_ec)
{
    RestrictInfo *rinfo;
    RestrictInfo *parent_rinfo = NULL;
    ListCell *lc;
    MemoryContext oldcontext;

    // Search existing clauses to avoid duplicates
    foreach(lc, ec->ec_sources) {
        rinfo = (RestrictInfo *) lfirst(lc);
        if ((rinfo->left_em == leftem && rinfo->right_em == rightem &&
             rinfo->parent_ec == parent_ec) ||
            (rinfo->left_em == rightem && rinfo->right_em == leftem &&
             rinfo->parent_ec == parent_ec))
            return rinfo;
    }

    foreach(lc, ec->ec_derives) {
        rinfo = (RestrictInfo *) lfirst(lc);
        if ((rinfo->left_em == leftem && rinfo->right_em == rightem &&
             rinfo->parent_ec == parent_ec) ||
            (rinfo->left_em == rightem && rinfo->right_em == leftem &&
             rinfo->parent_ec == parent_ec))
            return rinfo;
    }

    // Build new clause in planner context for reusability
    oldcontext = MemoryContextSwitchTo(root->planner_cxt);

    // Handle parent-child relationships for appendrel expansions
    if (leftem->em_is_child || rightem->em_is_child) {
        EquivalenceMember *leftp = leftem->em_parent ? leftem->em_parent : leftem;
        EquivalenceMember *rightp = rightem->em_parent ? rightem->em_parent : rightem;
        parent_rinfo = create_join_clause(root, ec, opno, leftp, rightp, parent_ec);
    }

    // Create the new join clause
    rinfo = build_implied_join_equality(root, opno, ec->ec_collation,
                                        leftem->em_expr, rightem->em_expr,
                                        bms_union(leftem->em_relids, rightem->em_relids),
                                        ec->ec_min_security);

    // Adjust clause_relids for child relations if needed
    if (leftem->em_is_child)
        rinfo->clause_relids = bms_add_members(rinfo->clause_relids, leftem->em_relids);
    if (rightem->em_is_child)
        rinfo->clause_relids = bms_add_members(rinfo->clause_relids, rightem->em_relids);

    // Copy parent's rinfo_serial for child clauses
    if (parent_rinfo)
        rinfo->rinfo_serial = parent_rinfo->rinfo_serial;

    // Set up clause metadata
    rinfo->parent_ec = parent_ec;
    rinfo->left_ec = ec;
    rinfo->right_ec = ec;
    rinfo->left_em = leftem;
    rinfo->right_em = rightem;

    // Save for potential reuse
    ec->ec_derives = lappend(ec->ec_derives, rinfo);

    MemoryContextSwitchTo(oldcontext);
    return rinfo;
}
```