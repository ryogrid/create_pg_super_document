# create_join_clause

## Location
src/backend/optimizer/path/equivclass.c: 1808 - 1991

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
  - build_implied_join_equality (to construct new RestrictInfo structures)
  - create_join_clause (recursive call for parent-child relationships)
  - bms_union (to combine relation bitmaps)
  - bms_add_members (to add relations to clause_relids)
  - EquivalenceClass, EquivalenceMember (struct types)
- Called from (representative examples):
  - generate_join_implied_equalities_normal
  - generate_implied_equalities_for_column
  - create_join_clause (recursive self-call)

## Notes and Other Information
- Returns existing RestrictInfo if a matching clause is found, otherwise creates a new one
- Handles commutative operators by checking both left-right and right-left operand arrangements
- Manages parent-child relationships for appendrel expansions by recursively creating parent clauses
- Uses planner memory context to ensure clause reusability across different planning phases
- The parent_ec parameter distinguishes between join clauses and restriction clauses for the same EM pair
- Automatically sets left_ec and right_ec to the provided EquivalenceClass to avoid additional lookups