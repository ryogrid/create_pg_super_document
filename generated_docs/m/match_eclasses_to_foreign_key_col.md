# match_eclasses_to_foreign_key_col

## Location
src/backend/optimizer/path/equivclass.c: 2500 - 2590

## Overview
Determines whether a specific column of a foreign key relationship is proven equal by any equivalence class, enabling query optimization opportunities based on foreign key constraints.

## Definition
```c
EquivalenceClass *match_eclasses_to_foreign_key_col(PlannerInfo *root,
                                                   ForeignKeyOptInfo *fkinfo,
                                                   int colno)
```

## Detailed Description
This function checks if the referenced and referencing variables of a foreign key's specified column are known to be equal through any equivalence class. Unlike exprs_known_equal, this function requires the comparison operator to exactly match the equivalence class's operator families, ensuring a definite (not approximate) equality relationship.

The function works by first identifying equivalence classes that mention both relations involved in the foreign key relationship. It then searches through the members of these equivalence classes to find Var nodes matching both the foreign key column and the referenced primary key column. When both are found in the same equivalence class, it verifies that the foreign key's equality operator is compatible with the equivalence class's operator families.

Upon successful match, the function updates the ForeignKeyOptInfo structure by setting both the equivalence class and the equivalence class member for the referencing variable, which can be used later in query optimization.

The function handles RelabelType nodes by unwrapping them to access the underlying Var nodes, and it skips volatile equivalence classes and child equivalence members to focus on stable, meaningful relationships.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing equivalence classes and relation information
- `fkinfo`: ForeignKeyOptInfo structure describing the foreign key relationship
- `colno`: Zero-based index of the foreign key column to examine

## Dependencies
- Functions called/Symbols referenced:
  - ForeignKeyOptInfo (struct type)
  - EquivalenceClass (struct type)  
  - [EquivalenceMember](../E/EquivalenceMember.md) (struct type)
  - [bms_intersect](../b/bms_intersect.md)
  - [bms_next_member](../b/bms_next_member.md)
  - [list_nth](../l/list_nth.md)
  - [get_mergejoin_opfamilies](../g/get_mergejoin_opfamilies.md)
  - [equal](../e/equal.md)
  - IS_SIMPLE_REL (macro)
  - RelabelType (node type)
- Called from (representative examples):
  - [match_foreign_keys_to_quals](match_foreign_keys_to_quals.md)
  - Referenced in paths.h header

## Notes and Other Information
- Returns the matching EquivalenceClass on success, NULL if no match is found
- Updates fkinfo->eclass[colno] and fkinfo->fk_eclass_member[colno] on successful match
- Only considers equivalence classes that mention both the referencing and referenced relations
- Requires exact operator family matching, making the result definite rather than approximate
- Skips volatile equivalence classes to avoid unreliable equality assumptions
- Handles RelabelType wrappers around Var nodes transparently
- The function assumes that equivalence class merging has been completed (ec_merging_done)
- Ignores child equivalence members, focusing on parent-level relationships
- Used primarily in foreign key optimization during query planning