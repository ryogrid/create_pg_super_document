# remove_rel_from_eclass

## Location
[src/backend/optimizer/plan/analyzejoins.c:622-675](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/analyzejoins.c#L622-L675)

## Overview
Removes any references to a specified relation ID or outer join relation ID from an EquivalenceClass data structure, maintaining consistency in the equivalence class membership and relid sets.

## Definition
```c
static void remove_rel_from_eclass(EquivalenceClass *ec, int relid, int ojrelid)
```

## Detailed Description
This function performs cleanup operations on an EquivalenceClass when a relation needs to be removed from query processing. It systematically removes references to the specified relation IDs from various components of the equivalence class:

1. Updates the EC's overall relid set by removing the specified relation IDs
2. Processes all equivalence members, removing relation references and deleting members that become empty
3. Cleans up source clauses by delegating to remove_rel_from_restrictinfo
4. Drops all derived clauses rather than attempting to fix them up

The function is designed to maintain the integrity of the equivalence class structure after relation removal, ensuring that join equalities will still be generated at appropriate join levels.

## Parameters / Member Variables
- `ec`: Pointer to the EquivalenceClass structure to be modified
- `relid`: The relation ID to be removed from the equivalence class
- `ojrelid`: The outer join relation ID to be removed from the equivalence class

## Dependencies
- Functions called/Symbols referenced:
  - [bms_del_member](../b/bms_del_member.md) (removes members from bitmapsets)
  - [bms_is_member](../b/bms_is_member.md) (checks bitmapset membership)
  - bms_is_empty (checks if bitmapset is empty)
  - foreach_delete_current (removes current list element during iteration)
  - [remove_rel_from_restrictinfo](remove_rel_from_restrictinfo.md) (cleans up RestrictInfo structures)
- Called from (representative examples):
  - [remove_rel_from_query](remove_rel_from_query.md)

## Notes and Other Information
- This is a static function within analyzejoins.c, indicating it's an internal helper function
- The function doesn't attempt to fix nullingrel bits in contained Vars and PHVs, which is noted as a potential future improvement
- Rather than fixing up derived clauses, the function simply drops them, as they would typically be base restriction clauses that are no longer needed
- The function maintains Assert checks to ensure that constants are not being processed inappropriately
- Located in src/backend/optimizer/plan/analyzejoins.c at lines 622-675

## Simplified Source

```c
static void remove_rel_from_eclass(EquivalenceClass *ec, int relid, int ojrelid)
{
    ListCell *lc;

    // Remove relation IDs from the equivalence class's overall relid set
    ec->ec_relids = bms_del_member(ec->ec_relids, relid);
    ec->ec_relids = bms_del_member(ec->ec_relids, ojrelid);

    // Clean up equivalence members
    foreach(lc, ec->ec_members) {
        EquivalenceMember *cur_em = (EquivalenceMember *) lfirst(lc);

        // If member references the removed relations
        if (bms_is_member(relid, cur_em->em_relids) ||
            bms_is_member(ojrelid, cur_em->em_relids)) {

            // Remove relation references from member
            cur_em->em_relids = bms_del_member(cur_em->em_relids, relid);
            cur_em->em_relids = bms_del_member(cur_em->em_relids, ojrelid);

            // Delete member if it has no remaining relations
            if (bms_is_empty(cur_em->em_relids))
                ec->ec_members = foreach_delete_current(ec->ec_members, lc);
        }
    }

    // Clean up source clauses
    foreach(lc, ec->ec_sources) {
        RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
        remove_rel_from_restrictinfo(rinfo, relid, ojrelid);
    }

    // Drop derived clauses rather than fixing them up
    ec->ec_derives = NIL;
}
```