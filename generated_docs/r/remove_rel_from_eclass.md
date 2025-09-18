# remove_rel_from_eclass

## Location
src/backend/optimizer/plan/analyzejoins.c: 622 - 675

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
  - bms_del_member (removes members from bitmapsets)
  - bms_is_member (checks bitmapset membership)
  - bms_is_empty (checks if bitmapset is empty)
  - foreach_delete_current (removes current list element during iteration)
  - remove_rel_from_restrictinfo (cleans up RestrictInfo structures)
- Called from (representative examples):
  - remove_rel_from_query

## Notes and Other Information
- This is a static function within analyzejoins.c, indicating it's an internal helper function
- The function doesn't attempt to fix nullingrel bits in contained Vars and PHVs, which is noted as a potential future improvement
- Rather than fixing up derived clauses, the function simply drops them, as they would typically be base restriction clauses that are no longer needed
- The function maintains Assert checks to ensure that constants are not being processed inappropriately
- Located in src/backend/optimizer/plan/analyzejoins.c at lines 622-675